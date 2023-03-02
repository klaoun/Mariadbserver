#ifndef SQL_MY_APC_INCLUDED
#define SQL_MY_APC_INCLUDED
/*
   Copyright (c) 2011, 2013 Monty Program Ab.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA */

/*
  Interface
  ~~~~~~~~~
   (
    - This is an APC request queue
    - We assume there is a particular owner thread which periodically calls
      process_apc_requests() to serve the call requests.
    - Other threads can post call requests, and block until they are exectued.
  )

  Implementation
  ~~~~~~~~~~~~~~
  - The target has a mutex-guarded request queue.

  - After the request has been put into queue, the requestor waits for request
    to be satisfied. The worker satisifes the request and signals the
    requestor.
*/

#include <atomic>
#include "my_atomic_wrapper.h"

class THD;

/*
  Target for asynchronous procedure calls (APCs). 
   - A target is running in some particular thread, 
   - One can make calls to it from other threads.
*/
class Apc_target
{
  mysql_mutex_t *LOCK_thd_kill_ptr;
public:
  Apc_target() : enabled(0), apc_calls(NULL) {} 
  ~Apc_target() { DBUG_ASSERT(!enabled && !apc_calls);}

  void init(mysql_mutex_t *target_mutex);

  /* Destroy the target. The target must be disabled when this call is made. */
  void destroy() { DBUG_ASSERT(!enabled); }

  /* Enter ther state where the target is available for serving APC requests */
  void enable() { enabled++; }

  /*
    Make the target unavailable for serving APC requests.

    @note
      This call will serve all requests that were already enqueued
  */
  void disable()
  {
    DBUG_ASSERT(enabled);
    mysql_mutex_lock(LOCK_thd_kill_ptr);
    bool process= !--enabled && have_apc_requests();
    mysql_mutex_unlock(LOCK_thd_kill_ptr);
    if (unlikely(process))
      process_apc_requests();
  }

  void process_apc_requests();
  /*
    A lightweight function, intended to be used in frequent checks like this:

      if (apc_target.have_requests()) apc_target.process_apc_requests()
  */
  inline bool have_apc_requests()
  {
    return MY_TEST(apc_calls.load(std::memory_order_acquire));
  }

  inline bool is_enabled() { return enabled; }
  
  /* Functor class for calls you can schedule */
  class Apc_call
  {
  public:
    /* This function will be called in the target thread */
    virtual void call_in_target_thread()= 0;
    virtual ~Apc_call() {}
  };

  class Call_request;
  /* Make a call in the target thread (see function definition for details) */
  bool make_apc_call(THD *caller_thd, Apc_call *call,
                     int timeout_sec, bool *timed_out);

  void enqueue_request(Call_request *request_buff, Apc_call *call);
  int wait_for_completion(THD *caller_thd, Call_request *request,
                           int timeout_sec);

#ifndef DBUG_OFF
  int n_calls_processed; /* Number of calls served by this target */
#endif
  // Epoch counter that increases before the command
  std::atomic<longlong> epoch {0};
  std::atomic<longlong> process_epoch {0};
private:

  /* 
    Non-zero value means we're enabled. It's an int, not bool, because one can
    call enable() N times (and then needs to call disable() N times before the 
    target is really disabled)
  */
  int enabled;

  /* 
    Circular, double-linked list of all enqueued call requests. 
    We use this structure, because we 
     - process requests sequentially: requests are added at the end of the 
       list and removed from the front. With circular list, we can keep one
       pointer, and access both front an back of the list with it.
     - a thread that has posted a request may time out (or be KILLed) and 
       cancel the request, which means we need a fast request-removal
       operation.
  */
  Atomic_relaxed<Call_request*> apc_calls;

public:
  class Call_request
  {
  public:
    Apc_call *call; /* Functor to be called */

    /* The caller will actually wait for "processed==TRUE" */
    bool processed;

    /* Condition that will be signalled when the request has been served */
    mysql_cond_t COND_request;
    mysql_mutex_t LOCK_request;
    
    /* Double linked-list linkage */
    Call_request *next;
    Call_request *prev;
    
    const char *what; /* (debug) state of the request */

    Call_request();
    ~Call_request();
  };
  void dequeue_request(Call_request *qe);
private:
  void enqueue_request(Call_request *qe);


  /* return the first call request in queue, or NULL if there are none enqueued */
  Call_request *get_first_in_queue()
  {
    mysql_mutex_assert_owner(LOCK_thd_kill_ptr);
    return apc_calls;
  }
};

#ifdef HAVE_PSI_INTERFACE
void init_show_explain_psi_keys(void);
#else
#define init_show_explain_psi_keys() /* no-op */
#endif


/**
  An idiom to protect coroutine-like (stateful) tasks that can process requests.
  Protects the coroutine (task) from skipping the signal while active and
  handles the resource reclamation problem.

  Both problems are going to be described in an iterative way.
  First, a lost signal problem will be addressed.

  Context:
  A task is executed in the thread pool and sometimes waits for data
  (user input, response from the remote server, etc). When it happens so, it is
  scheduled out, until the data is available.
  Typically:
    void Thread_pool::schedule_execution(Task *task) {
      get_queue()->put(task);
    }
  On the worker's side:
    Worker::execute(Task *task) {
      task->execute();
      // Enable the task back in the event poll.
      // When the data is ready, a task can be rescheduled.
      task->add_to_event_pool(
        [task](){ pool->schedule_execution(task); }
      );
    }
  Some other actors (threads, tasks) may make the requests to this
  task, for example to dump its state, or to change the state. But the
  task may sleep at that time. So the actor may want to wake it up.
    void send_request(Task *t, Request *r) {
      t->enqueue_request(r);
      t->wake_up_if_needed();
    }
  After such call, an actor would want to await on the response.

  Someone else (i.e. event pool or other actor) may also try to wake it up.
  How can we avoid double wake up and at the same time guarantee that a task
  will eventually process the messages?

  Three bool-returning functions are introduced for this:
  try_enter, try_leave, and notify.

  try_enter  Enters the execution context. Ensures the uniqueness of task
             presence in the execution pool. It simply sets the ENTER_BIT,
             which means entering critical section. If this bit wasn't set
             before, then task enters.
             @returns whether the task enters

  try_leave  ensures that the caller is aware of the pending events and leaves
             if none.
             First, it checks SIGNAL_BIT. If it's set, discards it and returns
             false. Else it discards ENTER_BIT and returns true.
             @returns true if task has left execution context.

  notify     sets the signal bit and exits. Returns true if the task
             should be explicitly woken up. It is determined by the presense of
             ENTER_BIT. If it was present, then it sohuld be handled by
             currently running task. If it's not, then the task
             should be woken up.
             @returns whether the task was active.

  try_leave usage protocol is as follows:
  void Task::leave() {
    while (!guard.try_leave())
      process_messages();
  }

  Now, a worker thread may discover this task finished and proceed to its
  deallocation. At this point, some other actor may access this task
  guard's state.

  Example:
  A task is typically added to a working queue with a method like this:
  void Thread_pool::schedule_execution(Task *task) {
    if (!task->guard.try_enter())
      return; // Do nothing as someone else executed this task.
    get_queue()->put(task);
  }

  Then on the Worker side:
  Worker::execute(Task *task) {
    assert_task_entered(task);
    task->execute();
    task->leave();

    // Add to the event pool (epoll/mitex queue/etc), where it can be
    // rescheduled. Normally, this normally be the last time when the task data
    // is accessed in this execution context, unless addition fails.
    bool success= task->add_to_event_pool(...);
    if (!success) {
      // Failure may mean a closed socket or file handle.
      delete task; // oops...
    }
  }

  It may go wrong by many ways!
  As one example, once the task executed leave(), any other actor may add it
  to the execution queue. Then, it may wrongly try to add the task to the event
  pool twice, and what's worse, end up in double free problem, or access to
  freed memory.

  To fix this, one may try to first disable the task in the event pool:
  void send_request(Task *t, Request *r) {
    assert_pointer_protected(t); // ensure nobody can delete this task
    t->enqueue_request(r);
    bool success;
    do {
      success = t->guard->notify() || t->remove_from_event_pool();
    } while (!success);
    release_pointer(t);
  }
  Few problems here:
  1. It's blocking.
  2. One also needs to protect from concurrent requesters. protect_pointer(t)
     could already do this, if it's for example a global mutex (or a chain of
     global -> local mutexes, like in MariaDB).
  3. It's not always possible to know whether we removed a task successfully
     or not. For example, this problem is present in epoll.


  The solution introduced here is a third bit representing ownership, and hence
  ownership passing.

  There's always the only owner. The ownership is first obtained with Task
  creation.
  An execution context is entered with try_enter_owner. If failed,
  then the ownership is atomically passed to the currently active execution
  context.
  On leaving the context, the ownership is checked. Once the owner
  leaves the context, no-one else can enter the context owned, until the
  ownership is passed again.
  An owner is responsible to free the resources.
  Only an owner can access resources without protection.

  So, one function, try_enter_owner is added, and try_leave function is updated
  with one new feature.

  try_enter_owner  enters the execution context by atomically setting
                   OWNER_BIt|ENTER_BIT. If ENTER_BIT was set in previous state
                   version, reports failure and passes the ownership by leaving
                   OWNER_BIT set.
                   @returns whether succeeds entering the execution context.

  try_leave        does either of the following:
                   * if SIGNAL_BIT is set, unsets it and reports failure
                   * otherwise leaves the execution context by unsetting
                     ENTER_BIT (and OWNER_BIT) and reports success
                   @returns enum with one of the following values:
                   SIGNAL     caller did not leave an execution context as there
                              was a signal. A false SIGNAL may be reported if
                              the ownership was passed in-between of try_leave's
                              work.
                   NOT_OWNER  caller has left an execution context is left and
                              by leaving he doesn't own the object
                   OWNER      execution context is left and the caller owns the
                              object


  Example:
    Worker::execute(Task *task) {
      assert_task_entered(task);
      task->execute();
      bool owner = task->leave();

      if (owner) {
        bool success= task->add_to_event_pool(...);
        if (!success) {
          // The task wasn't added to the event pool
(1)       if (task->try_enter_owner())
            delete task;
        }
      }
    }

    Now the task is only freed by the owner. Also, only owner adds the task back
    to the event pool. After that, event pool becomes the owner. Then, once the
    event is available, the task should be added with try_enter_owned:

    void Thread_pool::schedule_execution_owner(Task *task) {
      if (!task->guard.try_enter_owner())
        return; // Do nothing as someone else executed this task.
      get_queue()->put(task);
    }

    If try_enter_owner fails, i.e. condition on line (1) evaluates to false,
    then another (non-owner) context was active. Now it will become the owner
    and will be responsible for further resource deallocation.

    Third-party actors still require to ensure the protection on the pointer,
    as they do not own the task. The protection can be a mutex, or a one of
    memory reclamation schemes.
    The owner, in turn, should make precautions to make sure nobody uses this
    pointer. See for example THD::~THD().

    void send_request(Task *t) {
      assert_pointer_protected(t);
      t->enqueue_request(r);
      if (!t->guard->notify())
        pool->schedule_execution(t);
      release_pointer(t);
    }

    Note that a usual schedule_execution method is used here, which still enters
    context without ownership.

    Obviously, the request will be processed if either notify or enter succeed.

    If enter failed, then another context is currently active, but since notify
    failed, it wasn't active when request was enqueued. Therefore, it definitely
    will be processed.
 */
class Notifiable_work_zone
{
  static constexpr ulong ENTER_BIT = 4;
  static constexpr ulong OWNER_BIT = 2;
  static constexpr ulong SIGNAL_BIT = 1;
  std::atomic<ulong> state{0};
public:

  bool try_enter_owner()
  {
    const ulong ENTER_OWNER= ENTER_BIT|OWNER_BIT;
    ulong old_state= state.fetch_or(ENTER_OWNER);
    // We can't have an active owner in parallel.
    DBUG_ASSERT((old_state & ENTER_OWNER) != (ENTER_OWNER));
    return !(old_state & ENTER_BIT);
  }

  bool try_enter()
  {
    // assert mutex owner (lock_thd_kill)
    ulong old_state= state.fetch_or(ENTER_BIT);
    return !(old_state & ENTER_BIT);
  }
  bool has_event()
  {
    return state.load() & SIGNAL_BIT;
  }

  enum Leave_result
  {
    SIGNAL= 0,
    NOT_OWNER= 1,
    OWNER= 2,
  };
  static_assert(OWNER == OWNER_BIT, "OWNER_BIT changed?");

  Leave_result try_leave()
  {
    ulong old_state= state.load();
    DBUG_ASSERT(old_state & ENTER_BIT);

    if (unlikely(old_state & SIGNAL_BIT))
    {
      state.fetch_and(~SIGNAL_BIT);
      return SIGNAL; // one can reveal ownership only after leave
    }

    bool success= state.compare_exchange_weak(old_state, 0);
    return success
           // Transforms: OWNER_BIT == 2  ->  2 == SUCCESS_OWNER
           //                          0  ->  1 == SUCCESS_NOT_OWNER
           ? Leave_result(((old_state & OWNER_BIT) >> 1) + 1)
           : SIGNAL;

  }
  void discard()
  {
    ulong old_state= state.fetch_and(~SIGNAL_BIT);
    DBUG_ASSERT(old_state & ENTER_BIT);
  }
  bool notify()
  {
    ulong old_state= state.fetch_or(SIGNAL_BIT);
    return old_state & ENTER_BIT; // true if there was someone to notify
  }
};

#endif //SQL_MY_APC_INCLUDED

