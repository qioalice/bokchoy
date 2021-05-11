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

	"github.com/qioalice/ekago/v3/ekaerr"
	"github.com/qioalice/ekago/v3/ekatime"
	"github.com/qioalice/ekago/v3/ekatyp"
)

func (q *Queue) isValid() bool {
	return q != nil && q.wg != nil && q.parent != nil
}

// whyInvalid must be called only after isValid() was and only if isValid() return true.
// Reports why Queue object is considered invalid.
func (q *Queue) whyInvalid() string {
	switch {
	case q == nil:
		return "Queue receiver is nil"
	case q.wg == nil || q.parent == nil:
		return "Queue object has been instantiated manually instead of calling Bokchoy.Queue()"
	default:
		return "Internal error. Queue.whyInvalid()"
	}
}

func (q *Queue) onFunc(taskStatus TaskStatus, f HandlerFunc) *Queue {
	const s = "Bokchoy: Failed to register task status changed callbacks. "

	if !q.isValid() {
		return nil
	}

	var callbackDestination *[]HandlerFunc
	switch taskStatus {
	case TASK_STATUS_PROCESSING: callbackDestination = &q.onStart
	case TASK_STATUS_INVALID:    callbackDestination = &q.onComplete
	case TASK_STATUS_FAILED:     callbackDestination = &q.onFailure
	case TASK_STATUS_SUCCEEDED:  callbackDestination = &q.onSuccess
	}

	q.parent.sema.Lock()
	defer q.parent.sema.Unlock()

	if q.parent.isStarted {
		q.parent.logger.Copy().
			WithString("bokchoy_queue_name", q.name).
			Warn(s + "Consumers already running.")
		return q
	}

	//goland:noinspection GoNilness
	*callbackDestination = append(*callbackDestination, f)
	return q
}

// start starts consumers.
func (q *Queue) start() {
	const s = "Bokchoy: Failed to start queue. "

	// Do not lock/unlock q.parent.sema.
	// Already protected by callers.

	if q.options.Concurrency == 0 {
		q.parent.logger.Copy().
			WithString("bokchoy_queue_name", q.name).
			Warn(s + "Queue has no consumers. " +
				"Did you set negative/invalid concurrency option?")
		return
	}

	handlersCount :=
		len(q.handlers) +
		len(q.onStart) +
		len(q.onSuccess) +
		len(q.onFailure) +
		len(q.onComplete)

	if handlersCount == 0 {
		q.parent.logger.Copy().
			WithString("bokchoy_queue_name", q.name).
			Warn(s + "Queue has no registered handlers or task status changed callbacks. " +
				"Did you ever call Use() or any of " +
				"OnStart(), OnComplete(), OnFailure(), OnSuccess() setters?")
		return
	}

	q.consumers = make([]consumer, q.options.Concurrency)
	for i, n := 0, len(q.consumers); i < n; i++ {
		q.consumers[i].queue = q
		q.consumers[i].idx = int8(i)
		q.consumers[i].requestStart()
	}

	q.parent.logger.Copy().
		WithString("bokchoy_queue_name", q.name).
		WithInt("bokchoy_queue_consumers_number", len(q.consumers)).
		Debug("Bokchoy: Queue consumers has been started.")
}

// stop stops consumers.
func (q *Queue) stop() {

	// do not lock/unlock q.parent.sema.
	// Already protected by callers.

	if len(q.consumers) == 0 {
		return
	}

	for i, n := 0, len(q.consumers); i < n; i++ {
		q.consumers[i].requestStop()
	}

	q.wg.Wait()
	atomic.StoreInt32(&q.errCounter, 0)

	q.parent.logger.Copy().
		WithString("bokchoy_queue_name", q.name).
		WithInt("bokchoy_queue_consumers_number", len(q.consumers)).
		Debug("Bokchoy: Queue consumers has been stopped.")
}

func (q *Queue) decodeTasks(encodedTasks [][]byte) ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to decode many tasks using msgpack. "

	tasks := make([]Task, len(encodedTasks))
	for i, n := 0, len(encodedTasks); i < n; i++ {

		if err := tasks[i].Deserialize(encodedTasks[i], q.options.Serializer); err.IsNotNil() {
			return nil, err.AddMessage(s).
				WithString("bokchoy_queue_name", q.name).
				WithInt("bokchoy_decode_tasks_decoded", i).
				WithInt("bokchoy_decode_tasks_total", len(encodedTasks)).
				Throw()
		}
	}

	return tasks, nil
}

// save saves (creates or updates) a presented Task to the Queue's tasks list,
// w/o publishing it, meaning that this task won't available to consume,
// until it's not published explicitly.
func (q *Queue) save(t *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to save task. "

	if !q.isValid() {
		return ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	encodedTask, err := t.Serialize(q.options.Serializer)
	if err.IsNotNil() {
		return err.AddMessage(s).WithString("bokchoy_queue_name", q.name).Throw()
	}

	if t.IsFinished() {
		err = q.parent.broker.Set(q.name, t.id, encodedTask, t.TTL)
	} else {
		err = q.parent.broker.Set(q.name, t.id, encodedTask, 0)
	}
	if err.IsNotNil() {
		return err.AddMessage(s).
			WithString("bokchoy_queue_name", q.name).
			WithString("bokchoy_task_id", t.id).
			Throw()
	}

	q.parent.logger.Copy().
		WithString("bokchoy_queue_name", q.name).
		WithString("bokchoy_task_id", t.id).
		Debug("Bokchoy: Task has been saved")

	return nil
}

// newTask (unlike NewTask) allows DO NOT make unnecessary COPY of Option's slice.
// It's kinda micro optimisation.
//
// Even if Queue == nil, it's OK, cause nil Task will be returned.
// And it's
func (q *Queue) newTask(payload interface{}, options []Option) *Task {

	if !q.isValid() {
		return nil
	}

	optionsObject := q.options
	if len(options) > 0 {
		queueOptionsCopy := *q.options
		optionsObject = &queueOptionsCopy
		optionsObject.apply(options)
	}

	task := &Task{
		id:             ekatyp.ULID_New_OrNil().String(),
		status:         TASK_STATUS_WAITING,

		Payload:        payload,
		PublishedAt:    ekatime.Now(),

		MaxRetries:     optionsObject.MaxRetries,
		TTL:            optionsObject.TTL,
		Timeout:        optionsObject.Timeout,
		RetryIntervals: optionsObject.RetryIntervals,
	}

	if optionsObject.Countdown > 0 {
		task.ETA = time.Now().UnixNano() + optionsObject.Countdown.Nanoseconds()
	}

	return task
}
