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
	"time"

	"github.com/qioalice/ekago/v2/ekatime"
)

//goland:noinspection GoSnakeCaseUsage
const (
	_TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING = 10
)

func (t *Task) isValid() bool {
	return t != nil && t.id != "" && t.queueName != ""
}

func (t *Task) whyInvalid() string {
	switch {
	case t == nil:
		return "Task receiver is nil"
	case t.id == "" || t.queueName == "":
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
	t.MaxRetries--
	t.ETA = t.nextETA()
	t.status = TASK_STATUS_RETRYING
}

func (t *Task) markAsTimedOut() {
	t.processedAt = time.Now().UTC().UnixNano()
	t.status = TASK_STATUS_TIMED_OUT
}

// tillETA returns a time interval between now and Task's ETA.
// Returns 0 if ETA is not set.
//
// Requirements:
//  - Current Task is valid. Otherwise panic.
func (t *Task) tillETA() time.Duration {
	if t.ETA == 0 {
		return 0
	}
	return time.Duration(t.ETA - ekatime.Now()) * time.Second
}

// nextETA returns the next Task's ETA according with attempts counter.
func (t *Task) nextETA() ekatime.Timestamp {

	l := int8(len(t.RetryIntervals))

	if l == 0 {
		return 0
	}

	var retryInterval time.Duration

	if t.MaxRetries < l {
		retryInterval = t.RetryIntervals[l-t.MaxRetries]
	} else {
		retryInterval = t.RetryIntervals[0]
	}

	return ekatime.Now() + ekatime.Timestamp(retryInterval.Seconds())
}
