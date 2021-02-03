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

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekatime"
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
		if q.parent.logger.IsValid() {
			q.parent.logger.Warn(s + "Consumers already running.",
				"bokchoy_queue_name", q.name)
		}
		return q
	}

	//goland:noinspection GoNilness
	*callbackDestination = append(*callbackDestination, f)
	return q
}

// start starts consumers.
func (q *Queue) start() {

	// Do not lock/unlock q.parent.sema.
	// Already protected by callers.

	if q.options.Concurrency == 0 {
		if q.parent.logger.IsValid() {
			q.parent.logger.Warn("Bokchoy: Queue starting is requested " +
				"but does not have consumers. " +
				"Did you set negative/invalid concurrency option?",
				"bokchoy_queue_name", q.name)
		}
		return
	}

	handlersCount := len(q.handlers) +
		len(q.onStart) + len(q.onSuccess) + len(q.onFailure) + len(q.onComplete)
	if handlersCount == 0 {
		if q.parent.logger.IsValid() {
			q.parent.logger.Warn("Bokchoy: Queue starting is requested " +
				"but does not have registered handlers or task status changed callbacks. " +
				"Did you ever call Use() or any of " +
				"OnStart(), OnComplete(), OnFailure(), OnSuccess() setters?",
				"bokchoy_queue_name", q.name)
		}
		return
	}

	q.consumers = make([]consumer, q.options.Concurrency)
	for i, n := 0, len(q.consumers); i < n; i++ {
		q.consumers[i].queue = q
		q.consumers[i].idx = int8(i)
		q.consumers[i].requestStart()
	}

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Queue consumers has been started.",
			"bokchoy_queue_name", q.name,
			"bokchoy_queue_consumers_number", len(q.consumers))
	}
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

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Queue consumers has been stopped.",
			"bokchoy_queue_name", q.name,
			"bokchoy_queue_consumers_number", len(q.consumers))
	}
}

func (q *Queue) decodeTasks(encodedTasks [][]byte) ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to decode many tasks using msgpack. "

	tasks := make([]Task, len(encodedTasks))
	for i, n := 0, len(encodedTasks); i < n; i++ {

		err := tasks[i].Deserialize(encodedTasks[i], q.options.Serializer)
		if err.IsNotNil() {
			return nil, err.
				AddMessage(s).
				AddFields(
					"bokchoy_queue_name", q.name,
					"bokchoy_decode_tasks_decoded", i,
					"bokchoy_decode_tasks_total", len(encodedTasks)).
				Throw()
		}
	}

	return tasks, nil
}

// save saves a task to the queue.
func (q *Queue) save(task *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to save task. "

	if !q.isValid() {
		return ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	encodedTask, err := task.Serialize(q.options.Serializer)
	if err.IsNotNil() {
		return err.
			AddMessage(s).
			AddFields("bokchoy_queue_name", q.name).
			Throw()
	}

	if task.IsFinished() {
		err = q.parent.broker.Set(task.Key(), encodedTask, task.TTL)
	} else {
		err = q.parent.broker.Set(task.Key(), encodedTask, 0)
	}

	//goland:noinspection GoNilness
	if err.IsNotNil() {
		return err.
			AddMessage(s).
			AddFields(
				"bokchoy_queue_name", q.name,
				"bokchoy_task_id", task.id).
			Throw()
	}

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Task has been saved",
			"bokchoy_queue_name", q.name,
			"bokchoy_task_id", task.id)
	}

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
		id:          ID(),
		queueName:   q.name,
		Payload:     payload,
		status:      TASK_STATUS_WAITING,
		PublishedAt: ekatime.Now(),
	}

	task.MaxRetries     = optionsObject.MaxRetries
	task.TTL            = optionsObject.TTL
	task.Timeout        = optionsObject.Timeout
	task.RetryIntervals = optionsObject.RetryIntervals

	if optionsObject.Countdown > 0 {
		task.ETA = ekatime.Now() + ekatime.Timestamp(optionsObject.Countdown.Seconds())
	}

	return task
}
