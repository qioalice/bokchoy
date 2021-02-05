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
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
)

//goland:noinspection GoSnakeCaseUsage
const (
	_TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING = 10
)

func (t *Task) isValid() bool {
	return t != nil && t.id != ""
}

func (t *Task) whyInvalid() string {
	switch {
	case t == nil:
		return "Task receiver is nil"
	case t.id == "":
		return "Task object has been instantiated manually instead of calling Queue.NewTask()"
	default:
		return "Internal error. Task.whyInvalid()"
	}
}

func (t *Task) markAsFailed() {
	t.processedAt = time.Now().UTC().UnixNano()
	t.status = TASK_STATUS_FAILED
	t.ExecTime = time.Duration(t.processedAt - t.startedAt)
}

func (t *Task) markAsProcessing() {
	t.startedAt = time.Now().UTC().UnixNano()
	t.status = TASK_STATUS_PROCESSING
}

func (t *Task) markAsRetrying() {
	t.status = TASK_STATUS_RETRYING
}

func (t *Task) markAsTimedOut() {
	t.processedAt = time.Now().UTC().UnixNano()
	t.status = TASK_STATUS_TIMED_OUT
}

// nextETA returns the next Task's ETA according with MaxRetries attempts counter.
// Returns 0 if that counter is greater than length of RetryIntervals.
func (t *Task) nextETA() int64 {

	if l := int8(len(t.RetryIntervals)); l != 0 {
		var retryInterval time.Duration

		if t.MaxRetries < l {
			retryInterval = t.RetryIntervals[l-t.MaxRetries]
		} else {
			retryInterval = t.RetryIntervals[0]
		}

		return time.Now().UnixNano() + retryInterval.Nanoseconds()
	} else {
		return 0
	}
}

// fireSafeCall calls handler(t) protecting that call from the panic inside.
// It will be restored, saved into Panic, and corresponded error will be generated,
// and also saved to Error.
func (t *Task) fireSafeCall(handler HandlerFunc) {
	defer func(task *Task) {
		if task.Panic = recover(); task.Panic != nil {
			task.Error = ekaerr.IllegalState.
				New(fmt.Sprintf("Handler panicked: %+v", task.Panic)).
				Throw()
		}
	}(t)
	if err := handler(t); err.IsNotNil() {
		t.Error = err
	}
}

// fireMayContinue reports whether any other Task's handler may be called.
// It also may change status to the TASK_STATUS_RETRYING or TASK_STATUS_FAILED.
func (t *Task) fireMayContinue() bool {
	switch t.status {

	default:
		// Should never happen.
		// Left statuses:
		//
		//  - TASK_STATUS_INVALID:
		//    Cannot be assigned outside (Task.status is private field),
		//    and that status is never used at the assignation to Task.status.
		//
		// - TASK_STATUS_RETRYING:
		//   Once error is occurred, task.status is changed to TASK_STATUS_RETRYING
		//   and task must be returned to its pool until next iteration
		//   (after next ETA) if it's allowed by MaxRetries.
		//   So, anyway. For now an execution of task's handlers is done.
		//
		// - TASK_STATUS_TIMED_OUT:
		//   Should never be here if this is the Task's status.
		return false

	case TASK_STATUS_FAILED:
		// True, because task already failed.
		// Let the rest of onFailed callbacks be called.
		//
		// Even if onFailed callback returns error or even panicked,
		// what will we do? Change status to TASK_STATUS_FAILED? Heh.
		return true

	case TASK_STATUS_WAITING, TASK_STATUS_PROCESSING, TASK_STATUS_SUCCEEDED, TASK_STATUS_CANCELLED:
		if t.Error.IsNil() && t.Panic == nil {
			return true
		}
	}

	// We can be here only if either error or panic is occurred.
	if t.MaxRetries <= 0 {
		t.markAsFailed()
	} else {
		t.markAsRetrying()
	}

	return false
}
