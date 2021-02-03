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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
)

type (
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
	_CONSUMER_MAX_ERRORS_IN_A_ROW = 10

	_CONSUMER_STATUS_ACTIVE  int32 = 1
	_CONSUMER_STATUS_STOPPED int32 = 2
	_CONSUMER_STATUS_FREEZED int32 = 3
)

func (c *consumer) requestStart() {
	c.queue.wg.Add(1)
	if atomic.SwapInt32(&c.status, _CONSUMER_STATUS_ACTIVE) != _CONSUMER_STATUS_ACTIVE {
		go c.consumeLoop()
	}
}

func (c *consumer) requestStop() {
	atomic.StoreInt32(&c.status, _CONSUMER_STATUS_STOPPED)
}

func (c *consumer) consumeLoop() {

	defer c.queue.wg.Done()

	status := atomic.LoadInt32(&c.status)
	for status == _CONSUMER_STATUS_ACTIVE ||
		status == _CONSUMER_STATUS_FREEZED && c.idx == 0 {

		c.consumeLoopIter()
		status = atomic.LoadInt32(&c.status)
	}
}

func (c *consumer) consumeLoopIter() {

	tasks, err := c.queue.Consume()
	c.handleError(err)

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
		err = c.handleTask(&tasks[i])
		c.handleError(err)
	}
}

func (c *consumer) handleTask(task *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to handle task under consuming. "

	if c.queue.parent.logger.IsValid() {
		c.queue.parent.logger.Debug("Bokchoy: Task processing is started.",
			"bokchoy_queue_name", c.queue.name,
			"bokchoy_task_id", task.id)
	}

	if task.Timeout != 0 {

		var (
			timeoutTimer = time.NewTimer(task.Timeout)
			doneChan     = make(chan struct{})
		)

		// Invoke handlers asynchronously...
		go c.fire(doneChan, task)

		// ... but lock the consumer until task is processed
		// or until timeout comes.
		select {
		case _, _ = <- doneChan: // will be closed in c.invoker()
		case _, _ = <- timeoutTimer.C:
			task.markAsTimedOut()

			if c.queue.parent.logger.IsValid() {
				c.queue.parent.logger.Warn(s + "Timed out.",
					"bokchoy_task_id", task.id,
					"bokchoy_task_timeout", task.Timeout,
					"bokchoy_queue_name", c.queue.name)
			}
		}

		timeoutTimer.Stop() // GC timer

	} else {
		// Lock the consumer until task is processed.
		c.fire(nil, task)
	}

	// Maybe there is no need to save back to the broker, cause tash
	// already published and under retrying?

	if task.status == TASK_STATUS_RETRYING {
		return nil
	}

	if err := c.queue.save(task); err.IsNotNil() {
		return err.
			AddMessage(s + "Failed to save task after callbacks has been called " +
				"or timeout comes.").
			AddFields("bokchoy_task_status", task.status).
			Throw()
	}

	return nil
}

func (c *consumer) handleError(err *ekaerr.Error) {
	const s = "Bokchoy: An error (%d) occurred while trying to consume tasks of queue. "

	if err.IsNotNil() {

		queueConsumeErrorCounter := atomic.LoadInt32(&c.queue.errCounter)
		if queueConsumeErrorCounter >= _CONSUMER_MAX_ERRORS_IN_A_ROW {
			atomic.StoreInt32(&c.status, _CONSUMER_STATUS_FREEZED)
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

			atomic.StoreInt32(&c.status, _CONSUMER_STATUS_FREEZED)
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

		prevStatus := atomic.SwapInt32(&c.queue.errCounter, 0)
		unfreeze := prevStatus == _CONSUMER_STATUS_FREEZED && c.idx == 0

		// Shutdown of consumers may requested at this moment.
		// So, CAS is our protector. If it's not frozen atm, it's possible stopped.
		// So, don't do something in that case.

		if unfreeze {
			masterConsumerActivated := atomic.CompareAndSwapInt32(&c.status,
				_CONSUMER_STATUS_FREEZED, _CONSUMER_STATUS_ACTIVE)
			unfreeze = unfreeze && masterConsumerActivated
		}

		// We need to unfreeze others consumers
		// if there are more than 1 consumers.

		for i, n := 1, len(c.queue.consumers); unfreeze && i < n; i++ {

			slaveConsumerActivated := atomic.CompareAndSwapInt32(&c.queue.consumers[i].status,
				_CONSUMER_STATUS_FREEZED, _CONSUMER_STATUS_ACTIVE)
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

func (c *consumer) fire(done chan<- struct{}, task *Task) {

	defer func(done chan<- struct{}) {
		if done != nil {
			close(done)
		}
	}(done)

	task.markAsProcessing()

	// First of all call task status changed callbacks registered by
	// Queue.OnStart() func.
	c.fireEvents(task)

	// c.fireEvents() may call many status changed callbacks and its status
	// may be changed up to _TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING times.
	// But if it's still TASK_STATUS_PROCESSING, we need to call handlers.

	if task.status != TASK_STATUS_PROCESSING {
		return
	}

	oldStatus := task.status
	for i, n := 0, len(c.queue.handlers); i < n; i++ {
		c._firSafeCall(c.queue.handlers[i], task)

		if !c._firMayContinue(task) || oldStatus != task.status {
			break
		}
	}

	if task.status == TASK_STATUS_PROCESSING {
		task.MarkAsSucceeded()
	}

	// This is the last time we need to call c.fireEvents().
	// Assuming that now will be called callbacks registered only by
	//  - Either Queue.OnSuccess() or Queue.OnFailure()
	//  - And Queue.OnComplete().
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
		switch task.status {

		case TASK_STATUS_PROCESSING: handlers = c.queue.onStart
		case TASK_STATUS_SUCCEEDED:  handlers = c.queue.onSuccess
		case TASK_STATUS_FAILED:     handlers = c.queue.onFailure
		case TASK_STATUS_CANCELLED:  handlers = c.queue.onFailure
		}

		oldStatus = task.status

		for i, n := 0, len(handlers); i < n; i++ {
			c._firSafeCall(handlers[i], task)

			if !c._firMayContinue(task) || oldStatus != task.status {
				break
			}
		}

		if task.IsFinished() {
			oldStatus = task.status

			for i, n := 0, len(c.queue.onComplete); i < n; i++ {
				c._firSafeCall(c.queue.onComplete[i], task)

				if !c._firMayContinue(task) || oldStatus != task.status {
					break
				}
			}
		}

		if !c._firMayContinue(task) ||
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

// Functions started with "_fir" is a part of fire() call.
func (c *consumer) _firPanicProtector(task *Task) {
	if panicked := recover(); panicked != nil {
		task.Error = ekaerr.IllegalState.
			New(fmt.Sprintf("Handler panicked: %+v", panicked)).
			Throw()
	}
}

// Functions started with "_fir" is a part of fire() call.
func (c *consumer) _firSafeCall(handler HandlerFunc, task *Task) {
	defer c._firPanicProtector(task)
	if err := handler(task); err.IsNotNil() {
		task.Error = err
	}
}

// Functions started with "_fir" is a part of fire() call.
func (c *consumer) _firMayContinue(task *Task) bool {
	const s = "Bokchoy: Failed to republish task for being retried later. "

	switch task.status {

	default:
		// Should never happen.
		// Only TASK_STATUS_INVALID left, but it's cannot be assigned anywhere.
		return false

	case TASK_STATUS_FAILED:
		return true

	case TASK_STATUS_RETRYING, TASK_STATUS_CANCELLED, TASK_STATUS_TIMED_OUT:
		return false

	case TASK_STATUS_WAITING, TASK_STATUS_PROCESSING, TASK_STATUS_SUCCEEDED:
		if task.Error.IsNil() && task.Panic == nil {
			return true
		}
	}

	// It's continue of the case when task.status is one of
	// TASK_STATUS_WAITING, TASK_STATUS_PROCESSING, TASK_STATUS_SUCCEEDED,
	// but there is an error occurred (task.Error.IsNotNil() == true) or
	// a handler panicked and panic has been restored (task.Panic != nil).

	if task.MaxRetries <= 0 {
		task.markAsFailed()
		return false
	}

	task.markAsRetrying()

	if err := c.queue.PublishTask(task); err.IsNotNil() {
		task.markAsFailed()

		if c.queue.parent.logger.IsValid() {
			err.LogAsErrorUsing(c.queue.parent.logger, s +
				"Task will be marked as failed.")
		}
	}

	return false
}
