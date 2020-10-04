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

func (q *Queue) handleFunc(f HandlerFunc, options []Option) *Queue {
	const s = "Bokchoy: Failed to register handler for consuming queue. "

	if !q.isValid() {
		return nil
	}

	q.parent.sema.Lock()
	defer q.parent.sema.Unlock()

	switch {
	case len(q.consumers) > 0 && q.parent.logger.IsValid():
		q.parent.logger.Warn(s + "A queue already has a registered handler.",
			"bokchoy_queue_name", q.name)
		fallthrough

	case len(q.consumers) > 0:
		return q

	case q.parent.isStarted && q.parent.logger.IsValid():
		q.parent.logger.Warn(s + "Consumers already running.",
			"bokchoy_queue_name", q.name)
		fallthrough

	case q.parent.isStarted:
		return q
	}

	optionsObject := q.parent.defaultOptions
	if len(options) > 0 {
		queueOptionsCopy := *q.parent.defaultOptions
		optionsObject = &queueOptionsCopy
		optionsObject.apply(options)
	}

	for i := int8(0); i < optionsObject.Concurrency; i++ {
		q.consumers = append(q.consumers, consumer{
			idx: i,
			queue: q,
			handler: f,
		})
	}

	return q
}

// start starts consumers.
func (q *Queue) start() {

	// Do not lock/unlock q.parent.sema.
	// Already protected by callers.

	if len(q.consumers) == 0 {
		if q.parent.logger.IsValid() {
			q.parent.logger.Warn("Bokchoy: Queue starting is requested " +
				"but does not have consumers. " +
				"Did you set negative/invalid concurrency option?",
				"bokchoy_queue_name", q.name)
		}
		return
	}

	for i, n := 0, len(q.consumers); i < n; i++ {
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

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Queue consumers has been stopped.",
			"bokchoy_queue_name", q.name,
			"bokchoy_queue_consumers_number", len(q.consumers))
	}
}

func (q *Queue) fireEvents(task *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to call task status changed callbacks. "

	var callbacksToBeCalled *[]HandlerFunc

	oldStatus := TASK_STATUS_INVALID
	i := 0

	for ; oldStatus != task.status && i < _TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING; i++ {
		switch task.status {

		case TASK_STATUS_PROCESSING: callbacksToBeCalled = &q.onStart
		case TASK_STATUS_SUCCEEDED:  callbacksToBeCalled = &q.onSuccess
		case TASK_STATUS_FAILED:     callbacksToBeCalled = &q.onFailure
		case TASK_STATUS_CANCELLED:  callbacksToBeCalled = &q.onFailure
		}

		oldStatus = task.status

		//goland:noinspection GoNilness
		for i, n := 0, len(*callbacksToBeCalled); i < n; i++ {
			if err := (*callbacksToBeCalled)[i](task); err.IsNotNil() {
				return err.
					AddMessage(s).
					AddFields("bokchoy_task_status", oldStatus.String()).
					Throw()
			}
		}

		if task.IsFinished() {
			oldStatus = task.status
			for i, n := 0, len(q.onComplete); i < n; i++ {
				if err := q.onComplete[i](task); err.IsNotNil() {
					return err.
						AddMessage(s).
						AddFields("bokchoy_task_status", oldStatus.String()).
						Throw()
				}
			}
		}
	}

	if oldStatus != task.status && i == _TASK_MAX_STATUS_CHANGED_CALLBACKS_FIRING {
		return ekaerr.Interrupted.
			New(s + "Too many status changes.").
			AddFields("bokchoy_last_status", task.status.String()).
			Throw()
	}

	return nil
}

func (q *Queue) decodeTasks(encodedTasks [][]byte) ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to decode many tasks using msgpack. "

	tasks := make([]Task, len(encodedTasks))
	for i, n := 0, len(encodedTasks); i < n; i++ {

		err := tasks[i].Deserialize(encodedTasks[i], q.parent.serializer)
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

// newTask (unlike NewTask) allows DO NOT make unnecessary COPY of Option's slice.
// It's kinda micro optimisation.
//
// Even if Queue == nil, it's OK, cause nil Task will be returned.
// And it's
func (q *Queue) newTask(payload interface{}, options []Option) *Task {

	if !q.isValid() {
		return nil
	}

	optionsObject := q.parent.defaultOptions
	if len(options) > 0 {
		queueOptionsCopy := *q.parent.defaultOptions
		optionsObject = &queueOptionsCopy
		optionsObject.apply(options)
	}

	task := &Task{
		ID:          ID(),
		QueueName:   q.name,
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
