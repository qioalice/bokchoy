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
	"reflect"
	"sync"
	"unsafe"

	"github.com/qioalice/ekago/v2/ekaerr"

	"github.com/davecgh/go-spew/spew"
)

type (
	// Queue contains consumers to enqueue.
	Queue struct {

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

		errCounter     int32 // must be aligned, protected by atomic operations

		parent         *Bokchoy

		options        *Options

		name           string  // set by Bokchoy.Queue(), immutable
		consumers      []consumer
		wg             *sync.WaitGroup

		handlers       []HandlerFunc

		onFailure      []HandlerFunc
		onSuccess      []HandlerFunc
		onComplete     []HandlerFunc
		onStart        []HandlerFunc
	}
)

// Name returns the queue name.
func (q *Queue) Name() string {
	if !q.isValid() {
		return ""
	}
	return q.name
}

// Use appends a new handler middleware to the queue.
func (q *Queue) Use(handler ...HandlerFunc) *Queue {
	const s = "Bokchoy: Failed to register middleware for consuming queue. "

	if !q.isValid() {
		return nil
	}

	if len(handler) > 0 {
		// Filter middlewares. Remain only not-nil.
		handlersBeingRegistered := make([]HandlerFunc, 0, len(handler))
		for _, handlerBeingRegistered := range handlersBeingRegistered {
			if handlerBeingRegistered != nil {
				handlersBeingRegistered =
					append(handlersBeingRegistered, handlerBeingRegistered)
			}
		}
		handler = handlersBeingRegistered
	}

	// Now middlewares contains only not nil middlewares.

	if len(handler) == 0 {
		return q
	}

	q.parent.sema.Lock()
	defer q.parent.sema.Unlock()

	switch {
	case q.parent.isStarted && q.parent.logger.IsValid():
		q.parent.logger.Warn(s + "Consumers already running.",
			"bokchoy_queue_name", q.name)
		fallthrough

	case q.parent.isStarted:
		return q
	}

	// At the creation, Queue shares the middlewares with its Bokchoy (same RAM).
	// Thus we implementing copy-on-demand. And now is "demand".
	// But maybe it has been copied already?

	// WARNING!
	// DO NOT USE ekadanger.TakeRealAddr().
	// IT RETURNS A WRONG ADDRESSES IN THIS CASE!

	queueHandlersSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&q.handlers))
	bokchoyHandlersSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&q.parent.handlers))

	if queueHandlersSliceHeader.Data == bokchoyHandlersSliceHeader.Data {
		// https://github.com/go101/go101/wiki/How-to-perfectly-clone-a-slice
		q.handlers = append(q.parent.handlers[:0:0], q.parent.handlers...)
	}

	q.handlers = append(q.handlers, handler...)
	return q
}

// OnStart registers a new handler to be executed when a task is started.
func (q *Queue) OnStart(callback HandlerFunc) *Queue {
	return q.onFunc(TASK_STATUS_PROCESSING, callback)
}

// OnComplete registers a new handler to be executed when a task is completed.
func (q *Queue) OnComplete(callback HandlerFunc) *Queue {
	// TASK_STATUS_INVALID? OnCompleteFunc?
	// So, it's only for internal purposes.
	// I don't want to create another one constant that must covers two cases:
	//  - TASK_STATUS_SUCCEEDED,
	//  - TASK_STATUS_FAILED.
	// And do it just for route callbacks at the their registration.
	// Kinda bad solution, but I left here this comment for you.
	return q.onFunc(TASK_STATUS_INVALID, callback)
}

// OnFailure registers a new handler to be executed when a task is failed.
func (q *Queue) OnFailure(callback HandlerFunc) *Queue {
	return q.onFunc(TASK_STATUS_FAILED, callback)
}

// OnSuccess registers a new handler to be executed when a task is succeeded.
func (q *Queue) OnSuccess(callback HandlerFunc) *Queue {
	return q.onFunc(TASK_STATUS_SUCCEEDED, callback)
}

// Empty empties queue.
func (q *Queue) Empty() *ekaerr.Error {
	const s = "Bokchoy: Failed to empty queue. "

	if !q.isValid() {
		return ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	if err := q.parent.broker.Empty(q.name); err.IsNotNil() {
		return err.
			Throw()
	}

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Queue has been emptied",
			"bokchoy_queue_name", q.name)
	}

	return nil
}

// Cancel cancels a task using its ID.
func (q *Queue) Cancel(taskID string) (*Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to cancel the task. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()

	case taskID == "":
		return nil, ekaerr.IllegalArgument.
			New(s + "Task ID is empty.").
			AddFields("bokchoy_queue_name", q.name).
			Throw()
	}

	task, err := q.Get(taskID)
	if err != nil {
		return nil, err.
			AddMessage(s).
			Throw()
	}

	task.MarkAsCanceled()

	if err = q.save(task); err != nil {
		return nil, err.
			AddMessage(s).
			Throw()
	}

	return task, nil
}

// List returns tasks from the broker.
func (q *Queue) List() ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to retrieve all tasks from queue. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	encodedTasks, err := q.parent.broker.List(q.name)
	if err != nil {
		return nil, err.
			AddMessage(s).
			Throw()
	}

	tasks, err := q.decodeTasks(encodedTasks)
	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			Throw()
	}

	return tasks, nil
}

// Get returns a task instance from the broker with its id.
// Returns nil as Task if requested task is not found.
func (q *Queue) Get(taskID string) (*Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to retrieve task from queue by its ID. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()

	case taskID == "":
		return nil, ekaerr.IllegalArgument.
			New(s + "Task ID is empty.").
			AddFields("bokchoy_queue_name", q.name).
			Throw()
	}

	taskKey := TaskKey(q.name, taskID)

	encodedTask, err := q.parent.broker.Get(taskKey)
	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			AddFields(
				"bokchoy_queue_name", q.name,
				"bokchoy_task_id", taskID).
			Throw()
	}

	if encodedTask == nil {
		return nil, nil
	}

	task := new(Task)
	err = task.Deserialize(encodedTask, q.parent.serializer)
	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			AddFields(
				"bokchoy_queue_name", q.name,
				"bokchoy_task_key", taskKey)
	}

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Task has been retrieved",
			"bokchoy_queue_name", q.name,
			"bokchoy_task_id", task.id,
			"bokchoy_task_key", taskKey)
	}

	return task, nil
}

// Count returns statistics from queue:
// * direct: number of waiting tasks
// * delayed: number of waiting delayed tasks
// * total: number of total tasks
func (q *Queue) Count() (BrokerStats, *ekaerr.Error) {
	const s = "Bokchoy: Failed to get queue stat"

	if !q.isValid() {
		return BrokerStats{}, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	stats, err := q.parent.broker.Count(q.name)
	if err.IsNotNil() {
		return BrokerStats{}, err.
			AddMessage(s).
			AddFields("bokchoy_queue_name", q.name).
			Throw()
	}

	return stats, nil
}

// Consume returns an array of tasks.
func (q *Queue) Consume() ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to consume tasks from queue. "

	if !q.isValid() {
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	encodedTasks, err := q.parent.broker.Consume(q.name, 0)
	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			AddFields("bokchoy_queue_name", q.name).
			Throw()
	}

	tasks, err := q.decodeTasks(encodedTasks)
	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			Throw()
	}

	return tasks, nil
}

// NewTask returns a new Task instance from payload and options.
//
// Requirements:
// - Current Queue is valid. Otherwise nil Task is returned.
func (q *Queue) NewTask(payload interface{}, options ...Option) *Task {
	return q.newTask(payload, options)
}

// Publish is a alias for:
//
//     task := q.NewTask(ctx, payload, options...)
//     q.PublishTask(ctx, task)
//
func (q *Queue) Publish(payload interface{}, options ...Option) (*Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to publish task to the queue. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	task := q.newTask(payload, options)

	err := q.PublishTask(task)
	if err != nil {
		return nil, err.
			Throw()
	}

	return task, nil
}

// PublishTask publishes a new task to the current Queue.
func (q *Queue) PublishTask(task *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to publish task to the queue. "
	switch {

	case !q.isValid():
		return ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	// No need to check task,
	// because task.Serialize already has all checks.

	serializedTask, err := task.Serialize(q.parent.serializer)
	if err.IsNotNil() {
		return err.
			AddMessage(s).
			AddFields("bokchoy_queue_name", q.name).
			Throw()
	}

	err = q.parent.broker.Publish(q.name, task.id, serializedTask, task.ETA)
	if err.IsNotNil() {
		return err.
			AddMessage(s).
			AddFields(
				"bokchoy_queue_name", q.name,
				"bokchoy_task_id", task.id,
				"bokchoy_task_user_payload", spew.Sdump(task.Payload)).
			Throw()
	}

	if q.parent.logger.IsValid() {
		q.parent.logger.Debug("Bokchoy: Tash has been published",
			"bokchoy_queue_name", q.name,
			"bokchoy_task_id", task.id,
			"bokchoy_task_user_payload", spew.Sdump(task.Payload))
	}

	return nil
}
