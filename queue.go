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

	"github.com/qioalice/ekago/v3/ekaerr"

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

		options        *options

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
func (q *Queue) Use(callback ...HandlerFunc) *Queue {
	const s = "Bokchoy: Failed to register middleware for consuming queue. "

	if !q.isValid() {
		return nil
	}

	if len(callback) > 0 {
		// Filter handlers. Remain only not-nil.
		handlersBeingRegistered := make([]HandlerFunc, 0, len(callback))
		for _, handlerBeingRegistered := range callback {
			if handlerBeingRegistered != nil {
				handlersBeingRegistered =
					append(handlersBeingRegistered, handlerBeingRegistered)
			}
		}
		callback = handlersBeingRegistered
	}

	// Now 'callback' contains only not nil handlers.

	if len(callback) == 0 {
		return q
	}

	q.parent.sema.Lock()
	defer q.parent.sema.Unlock()

	if q.parent.isStarted {
		q.parent.logger.Copy().
			WithString("bokchoy_queue_name", q.name).
			Warnw(s + "Consumers already running.")
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

	q.handlers = append(q.handlers, callback...)
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
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	if err := q.parent.broker.Empty(q.name); err.IsNotNil() {
		return err.AddMessage(s).Throw()
	}

	q.parent.logger.Copy().
		WithString("bokchoy_queue_name", q.name).
		Debug("Bokchoy: Queue has been emptied")

	return nil
}

// Cancel cancels a task using its ID.
func (q *Queue) Cancel(taskID string) (*Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to cancel the task. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()

	case taskID == "":
		return nil, ekaerr.IllegalArgument.
			New(s + "Task ID is empty.").
			WithString("bokchoy_queue_name", q.name).
			Throw()
	}

	task, err := q.Get(taskID)

	if err.IsNil() {
		task.MarkAsCanceled()
		err = q.save(task)
	}

	return task, err.AddMessage(s).Throw()
}

// List returns tasks from the broker.
func (q *Queue) List() ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to retrieve all tasks from queue. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	encodedTasks, err := q.parent.broker.List(q.name)
	if err.IsNotNil() {
		return nil, err.AddMessage(s).Throw()
	}

	tasks, err := q.decodeTasks(encodedTasks)
	if err.IsNotNil() {
		return nil, err.AddMessage(s).Throw()
	}

	return tasks, nil
}

// Get returns a Task instance from the current Broker's Queue with its id.
// Returns nil as Task if requested task is not found.
func (q *Queue) Get(taskID string) (*Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to retrieve task from queue by its ID. "
	switch {

	case !q.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()

	case taskID == "":
		return nil, ekaerr.IllegalArgument.
			New(s + "Task ID is empty.").
			WithString("bokchoy_queue_name", q.name).
			Throw()
	}

	var task Task

	encodedTask, err := q.parent.broker.Get(q.name, taskID)

	if err.IsNil() && len(encodedTask) == 0 {
		return nil, nil
	}

	if err.IsNil() {
		err = task.Deserialize(encodedTask, q.options.Serializer)
	}

	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			WithString("bokchoy_queue_name", q.name).
			WithString("bokchoy_task_id", taskID).
			Throw()
	}

	q.parent.logger.Copy().
		WithString("bokchoy_queue_name", q.name).
		WithString("bokchoy_task_id", task.id).
		Debug("Bokchoy: Task has been retrieved")

	return &task, nil
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
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	stats, err := q.parent.broker.Count(q.name)
	return stats, err.AddMessage(s).WithString("bokchoy_queue_name", q.name).Throw()
}

// Consume returns an array of tasks.
func (q *Queue) Consume() ([]Task, *ekaerr.Error) {
	const s = "Bokchoy: Failed to consume tasks from queue. "

	if !q.isValid() {
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	var tasks []Task

	encodedTasks, err := q.parent.broker.Consume(q.name, 0)

	if err.IsNil() {
		tasks, err = q.decodeTasks(encodedTasks)
	} else {
		err.WithString("bokchoy_queue_name", q.name)
	}

	return tasks, err.AddMessage(s).Throw()
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

	if !q.isValid() {
		return nil, ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	task := q.newTask(payload, options)
	err := q.PublishTask(task)

	return task, err.Throw()
}

// PublishTask publishes a new task to the current Queue.
func (q *Queue) PublishTask(task *Task) *ekaerr.Error {
	const s = "Bokchoy: Failed to publish task to the queue. "

	if !q.isValid() {
		return ekaerr.IllegalArgument.
			New(s + "Queue is invalid. Has it been initialized correctly?").
			WithString("bokchoy_queue_why_invalid", q.whyInvalid()).
			Throw()
	}

	// No need to check task,
	// because task.Serialize already has all checks.

	serializedTask, err := task.Serialize(q.options.Serializer)
	if err.IsNotNil() {
		return err.AddMessage(s).WithString("bokchoy_queue_name", q.name).Throw()
	}

	if err = q.parent.broker.Publish(q.name, task.id, serializedTask, task.ETA); err.IsNotNil() {
		return err.AddMessage(s).
			WithString("bokchoy_queue_name", q.name).
			WithString("bokchoy_task_id", task.id).
			WithString("bokchoy_task_user_payload", spew.Sdump(task.Payload)).
			Throw()
	}

	q.parent.logger.Copy().
		WithString("bokchoy_queue_name", q.name).
		WithString("bokchoy_task_id", task.id).
		//WithString("bokchoy_task_user_payload", spew.Sdump(task.Payload)).
		Debug("Bokchoy: Task has been published")

	return nil
}
