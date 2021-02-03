//
// ORIGINAL PACKAGE
// ( https://github.com/thoas/bokchoy )
//
//     Copyright © 2019. All rights reserved.
//     Author: Florent Messa
//     Contacts: florent.messa@gmail.com, https://github.com/thoas
//     License: https://opensource.org/licenses/MIT
//
// HAS BEEN FORKED, HIGHLY MODIFIED AND NOW IS AVAILABLE AS
// ( https://github.com/qioalice/bokchoy )
//
//     Copyright © 2020. All rights reserved.
//     Author: Ilya Stroy.
//     Contacts: qioalice@gmail.com, https://github.com/qioalice
//     License: https://opensource.org/licenses/MIT
//

package bokchoy

import (
	"sync/atomic"
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
)

type (
	// consumer is a some Queue's Task s executioner.
	// One Queue will have as many consumers,
	// as requested by WithConcurrency() option. Default is: _DEFAULT_CONCURRENCY.
	//
	// Main consumer's loop is started by requestStart() method,
	// stops by requestStop()'s one,
	// but it may stop itself if it's not master consumer.
	//
	// Master consumer it's a consumer with idx == 0.
	// idx is sets by Queue at the consumer's creation.
	// All other consumers are considered slaves.
	//
	// Master/Slave division of consumers is exist
	// to decrease load to the either Broker, or any your service
	// when an errors occurred one-by-one in a row, counting not only Broker errors,
	// but Task's callbacks/handlers too.
	//
	// Don't worry, it's OK if sometimes your Task s are failed,
	// or Broker returns an errors (while retrieving/saving tasks).
	// But it's not, if there's ONLY errors, nothing more.
	// Each task, each Broker's call, everything is failed.
	//
	// It guarantees, that NOT MORE THAN ONE loop may be run for one consumer.
	consumer struct {

		// WARNING!
		// DO NOT CHANGE THE ORDER OF FIELDS!
		// https://golang.org/pkg/sync/atomic/#pkg-note-BUG :
		//
		//   > On ARM, x86-32, and 32-bit MIPS,
		//   > it is the caller's responsibility to arrange for 64-bit alignment
		//   > of 64-bit words accessed atomically.
		//   > The first word in a variable or in an allocated struct, array,
		//   > or slice can be relied upon to be 64-bit aligned.
		//
		// Also:
		// https://stackoverflow.com/questions/28670232/atomic-addint64-causes-invalid-memory-address-or-nil-pointer-dereference/51012703#51012703

		status     int32 // protected by atomic operations

		idx        int8
		queue      *Queue
	}
)

//goland:noinspection GoSnakeCaseUsage
const (
	// TODO: Add an Option to manage this value instead of constant usage.
	_CONSUMER_MAX_ERRORS_IN_A_ROW = 32

	_CONSUMER_STATUS_ACTIVE  int32 = 1
	_CONSUMER_STATUS_STOPPED int32 = 2
	_CONSUMER_STATUS_FROZEN  int32 = 3
)

// requestStart starts consumer's loop (if it's not started yet)
// calling consumeLoop() in a new, separated goroutine.
func (c *consumer) requestStart() {
	c.queue.wg.Add(1)
	if atomic.SwapInt32(&c.status, _CONSUMER_STATUS_ACTIVE) != _CONSUMER_STATUS_ACTIVE {
		go c.consumeLoop()
	}
}

// requestStop() asks consumer to stop its loop.
// IT DOES NOT STOP LOOP IMMEDIATELY.
func (c *consumer) requestStop() {
	atomic.StoreInt32(&c.status, _CONSUMER_STATUS_STOPPED)
}

// consumeLoop() is consumer's loop. It just calls consumeLoopIter(),
// checking whether loop can go to the next iteration after each one.
func (c *consumer) consumeLoop() {
	defer c.queue.wg.Done()

	status := atomic.LoadInt32(&c.status)
	for status == _CONSUMER_STATUS_ACTIVE ||
		status == _CONSUMER_STATUS_FROZEN && c.idx == 0 {

		c.consumeLoopIter()
		status = atomic.LoadInt32(&c.status)
	}
}

// consumerLoopIter is consumer's loop iteration.
//
// It tries to retrieve next N tasks (depends of Broker.Consume())
// and process all of them one-by-one using processTask() method.
func (c *consumer) consumeLoopIter() {

	tasks, err := c.queue.Consume()
	c.countErrorIfAny(err)

	if len(tasks) == 0 {
		return
	}

	if c.queue.parent.logger.IsValid() {
		c.queue.parent.logger.Debug("Bokchoy: Received tasks to consume.",
			"bokchoy_queue_name", c.queue.name,
			"bokchoy_tasks_received", len(tasks),
			"bokchoy_queue_consumsers_idx", c.idx,
			"bokchoy_queue_consumers_number", len(c.queue.consumers))
	}

	for i, n := 0, len(tasks); i < n; i++ {
		err = c.processTask(&tasks[i])
		c.countErrorIfAny(err)
	}
}

// processTask is the Task's processing entry point.
// After this, Task may be only:
//  - Returned back to the pool if it must be retried later,
//  - Considering completed (w/ or w/o error/panic).
//
// Locks itself until Task is reached any of two states above, or till Task.Timeout.
// If Task.Timeout is presented and reached,
// no neither next callbacks is called nor handlers, but the last callback/handler
// that might be under execution keeps locking separated goroutine,
// until it's done.
func (c *consumer) processTask(t *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to process task under consuming. "

	if c.queue.parent.logger.IsValid() {
		c.queue.parent.logger.Debug("Bokchoy: Task processing is started.",
			"bokchoy_queue_name", c.queue.name,
			"bokchoy_task_id",    t.id)
	}

	if t.Timeout != 0 {
		var (
			timeoutTimer = time.NewTimer(t.Timeout)
			doneChan     = make(chan struct{})
		)

		// Handlers and callbacks will be called in another goroutine,
		// but all in the same.
		go c.fire(doneChan, t)

		select {
		case _, _ = <- doneChan: // will be closed in c.fire()
		case _, _ = <- timeoutTimer.C:
			t.markAsTimedOut()
			if c.queue.parent.logger.IsValid() {
				c.queue.parent.logger.Warn(s + "Timed out.",
					"bokchoy_task_id",      t.id,
					"bokchoy_task_timeout", t.Timeout,
					"bokchoy_queue_name",   c.queue.name)
			}
		}

		timeoutTimer.Stop() // GC timer
	} else {
		c.fire(nil, t)
	}

	if t.status == TASK_STATUS_RETRYING {

		// FIRST: calculating next ETA, THEN: decreasing MaxRetries.
		// NOT VICE VERSA! PANIC OTHERWISE, INDEX OUT OF RANGE IN (*Task).nextETA().
		t.ETA = t.nextETA()
		t.MaxRetries--

		if err := c.queue.save(t); err.IsNotNil() {
			return err.
				AddMessage(s + "Failed to return failed task to the pool " +
					"for being retried later.").
				Throw()
		}
	}

	return nil
}

// countErrorIfAny implements 95% of starting/stopping/freezing master/slave
// consumer s when error is occurred.
//
// It freezes all slave consumer s if Queue.errCounter is reached N
// (Default is: _CONSUMER_MAX_ERRORS_IN_A_ROW).
// It also unfreezes all slave consumer s if master consumer meet non-error call;
// and counts all errors using Queue.errCounter.
//
// Writes logs, handles rare cases like stopping requested while freezing/unfreezing,
// etc.
func (c *consumer) countErrorIfAny(err *ekaerr.Error) {
	const s = "Bokchoy: An error (%d) occurred while trying to consume tasks of queue. "

	if err.IsNotNil() {

		queueConsumeErrorCounter := atomic.LoadInt32(&c.queue.errCounter)
		if queueConsumeErrorCounter >= _CONSUMER_MAX_ERRORS_IN_A_ROW {
			// CAS because we don't want to change stopped -> frozen.
			// It should never happen at all, but who knows the future, right?
			atomic.CompareAndSwapInt32(&c.status,
				_CONSUMER_STATUS_ACTIVE, _CONSUMER_STATUS_FROZEN)
			return
		}

		queueConsumeErrorCounter = atomic.AddInt32(&c.queue.errCounter, 1)
		if queueConsumeErrorCounter >= _CONSUMER_MAX_ERRORS_IN_A_ROW {
			queueConsumeErrorCounter = _CONSUMER_MAX_ERRORS_IN_A_ROW

			if c.queue.parent.logger.IsValid() {
				err.LogAsErrorUsing(c.queue.parent.logger, s +
					"Error limit is reached. " +
					"All consumers but one will be freeze until error is get out.",
					queueConsumeErrorCounter)
			}

			atomic.StoreInt32(&c.status, _CONSUMER_STATUS_FROZEN)
			return
		} else {

			if c.queue.parent.logger.IsValid() {
				err.LogAsErrorUsing(c.queue.parent.logger, s +
					"Another %d errors and all but one consumers will be temporary stopped.",
					queueConsumeErrorCounter,
					_CONSUMER_MAX_ERRORS_IN_A_ROW - queueConsumeErrorCounter)
			}
		}
	} else {

		// Reset Queue's error counter.
		atomic.StoreInt32(&c.queue.errCounter, 0)

		// Whether it's master consumer and it must unfreeze others?
		unfreeze :=
			atomic.LoadInt32(&c.queue.errCounter) == _CONSUMER_STATUS_FROZEN &&
			c.idx == 0

		// Shutdown of consumers may requested at this moment.
		// So, CAS is our protector.
		// If it's not frozen atm, it's possible stopped.
		// So, don't do something in that case.

		if unfreeze {
			masterConsumerActivated := atomic.CompareAndSwapInt32(&c.status,
				_CONSUMER_STATUS_FROZEN, _CONSUMER_STATUS_ACTIVE)
			unfreeze = unfreeze && masterConsumerActivated
		}

		// We need to unfreeze others consumers,
		// if there are more than 1 consumers.

		for i, n := 1, len(c.queue.consumers); unfreeze && i < n; i++ {

			slaveConsumerActivated := atomic.CompareAndSwapInt32(&c.queue.consumers[i].status,
				_CONSUMER_STATUS_FROZEN, _CONSUMER_STATUS_ACTIVE)
			if slaveConsumerActivated {
				c.queue.consumers[i].requestStart()
			}
			unfreeze = unfreeze && slaveConsumerActivated

			if !slaveConsumerActivated {
				// Very rare case.
				// The stopping has been requested while we unfreezing consumers.
				// Apply the current consumer's status to the prev ones
				applicableStatus := atomic.LoadInt32(&c.queue.consumers[i].status)
				for j := 1; j < i; j++ {
					atomic.StoreInt32(&c.queue.consumers[i].status, applicableStatus)
				}
			}
		}
	}
}

// fire calls passed Task's handlers and callbacks, changing its status,
// according with Task's state while execution callbacks.
// Closes passed channel if it's not nil when fire is done.
func (c *consumer) fire(done chan<- struct{}, task *Task) {

	defer func(done chan<- struct{}) {
		if done != nil {
			close(done)
		}
	}(done)

	task.markAsProcessing()

	// First of all call onStart callbacks.
	c.fireEvents(task)

	// c.fireEvents() may call many status changed callbacks and its status
	// may be changed up to _TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING times.
	//
	// But if it's still TASK_STATUS_PROCESSING, we need to call handlers.
	if task.status != TASK_STATUS_PROCESSING {
		return
	}

	oldStatus := task.status
	for i, n := 0, len(c.queue.handlers); i < n; i++ {
		task.fireSafeCall(c.queue.handlers[i])
		if !task.fireMayContinue() || oldStatus != task.status {
			break
		}
	}

	if task.status == TASK_STATUS_PROCESSING {
		task.MarkAsSucceeded()
	}

	// This is the last time we need to call c.fireEvents().
	// Expecting only onSuccess, onFailure callbacks will be called now.
	c.fireEvents(task)
}

func (c *consumer) fireEvents(task *Task) {
	const s = "Bokchoy: Failed to call task status changed callbacks. "

	var (
		handlers  []HandlerFunc
		oldStatus TaskStatus
		i         = 0
	)

	for ; i < _TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING; i++ {

		switch ts := task.status; {
		case ts == TASK_STATUS_PROCESSING: handlers = c.queue.onStart
		case ts == TASK_STATUS_SUCCEEDED:  handlers = c.queue.onSuccess
		case ts == TASK_STATUS_FAILED:     handlers = c.queue.onFailure
		case ts == TASK_STATUS_CANCELLED:  handlers = c.queue.onFailure
		case task.IsFinished():            handlers = c.queue.onComplete
		}

		oldStatus = task.status

		// Starting execution one-by-one all handlers for the current Task's status,
		// until they are over and while Task's status is not changed.
		for i, n := 0, len(handlers); i < n; i++ {
			task.fireSafeCall(handlers[i])
			if !task.fireMayContinue() || oldStatus != task.status {
				break
			}
		}

		if !task.fireMayContinue() ||
				oldStatus == task.status ||
				task.status == TASK_STATUS_RETRYING {
			break
		}
	}

	if i < _TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING ||
			!c.queue.parent.logger.IsValid() {
		return
	}

	ekaerr.Interrupted.
		New("Too many status changes.").
		AddFields("bokchoy_last_status", task.status.String()).
		LogAsWarnUsing(c.queue.parent.logger, s,
			"bokchoy_task_id", task.id,
			"bokchoy_queue_name", c.queue.name)
}
