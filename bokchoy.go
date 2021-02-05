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
	"strings"
	"sync"

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekalog"
	"github.com/qioalice/ekago/v2/ekaunsafe"
)

type (
	// Bokchoy is the main object which stores all configuration,
	// queues and broker.
	Bokchoy struct {
		sema           *sync.Mutex
		wg             *sync.WaitGroup
		defaultOptions *options
		broker         Broker
		queues         map[string]*Queue

		handlers    []HandlerFunc

		logger         *ekalog.Logger
		isStarted      bool

		queueNamesWithDuplicateHandlers []string
	}
)

// New initializes a new Bokchoy instance.
func New(options ...Option) (*Bokchoy, *ekaerr.Error) {

	defaultOptionsCopy := *defaultOptions
	optionsObject := &defaultOptionsCopy

	for i, n := 0, len(options); i < n; i++ {
		if options[i] != nil {
			options[i](optionsObject)
		}
	}

	// Validate options.
	// Some options are must presented by user.
	switch {

	case optionsObject.Broker == nil:
		return nil, ekaerr.InitializationFailed.
			New("Bokchoy: Broker must be presented. " +
				"Use WithBroker() option as a part of constructor argument").
			Throw()

	case optionsObject.Serializer == nil:
		return nil, ekaerr.InitializationFailed.
			New("Bokchoy: Serializer must be presented. " +
				"Use WithSerializer() option as a part of constructor argument").
			Throw()
	}

	// Validate options part 2.
	// Now it's only warning messages.

	logger := ekalog.WithThis() // an easy way to get default logger
	// User can "disable" logging passing nil or invalid logger.
	// Thus there is no either nil check nor logger.IsValid() call.
	if optionsObject.loggerIsPresented {
		logger = optionsObject.Logger
	}

	if ekaunsafe.TakeRealAddr(optionsObject.Broker) == nil && logger.IsValid() {
		logger.Warn("Bokchoy.Initialization: " +
			"You present a Broker with nil underlying address value. It's ok?")
	}

	if ekaunsafe.TakeRealAddr(optionsObject.Serializer) == nil && logger.IsValid() {
		logger.Warn("Bokchoy.Initialization: " +
			"You present a Serializer with nil underlying address value. It's ok?")
	}

	if !optionsObject.Initialize && logger.IsValid() {
		logger.Warn("Bokchoy.Initialization: " +
			"You did not present initialize option (WithInitialize(true)). " +
			"Do you already initialize your broker manually?")
	}

	// todo TTL <= 0 -> immortal tasks overflow RAM

	// Options has been validated.
	// It's OK and safe to proceed.

	if optionsObject.Initialize {

		if logger.IsValid() {
			logger.Debug("Bokchoy.Initialization: " +
				"Initialize presented Broker...",
				"bokchoy_broker", optionsObject.Broker.String())
		}

		if logger.IsValid() {
			logger.Debug("Bokchoy.Initialization: " +
				"Initialized successfully. Ready to use.",
				"bokchoy_broker", optionsObject.Broker.String())
		}
	}

	bok := &Bokchoy{
		broker:         optionsObject.Broker,
		queues:         make(map[string]*Queue),
		wg:             &sync.WaitGroup{},
		sema:           &sync.Mutex{},
		logger:         logger,
		defaultOptions: optionsObject,
	}

	for i, n := 0, len(optionsObject.Queues); i < n; i++ {
		if optionsObject.Queues[i] != "" {
			_ = bok.Queue(optionsObject.Queues[i])
		}
	}

	if !optionsObject.DisableOutput {
		bok.displayOutput()
	}

	return bok, nil
}

// Queue gets or creates a new.
//
// If Run() has been called already, the new queue's consumers will be start
// immediately (if it's a new queue, and if Bokchan has not been stopped yet).
//
// If queue with the given 'name' has already declared,
// the Queue method just returns it, but if at least one Option is provided
// (even nil), the queue will be recreated with provided options.
func (b *Bokchoy) Queue(name string, options ...Option) *Queue {

	if !b.isValid() {
		return nil
	}

	b.sema.Lock()
	defer b.sema.Unlock()

	if b.isStarted {
		// TODO: Run queue's consumers immediately
		return nil
	}

	optionsObject := b.defaultOptions
	if len(options) > 0 {
		bokchoyDefaultOptionsCopy := *b.defaultOptions
		optionsObject = &bokchoyDefaultOptionsCopy
		optionsObject.apply(options)
	}

	queue, ok := b.queues[name]
	if !ok {
		queue = &Queue{
			parent:   b,
			options:  optionsObject,
			name:     name,
			wg:       b.wg,
			handlers: b.handlers,
		}

		b.queues[name] = queue
	}

	return queue
}

// Run runs the system and block the current goroutine.
func (b *Bokchoy) Run() *ekaerr.Error {

	if !b.isValid() {
		return ekaerr.InitializationFailed.
			New("Bokchoy: Bokchoy is not initialized. " +
				"Did you just create an object instead of using constructor or initializer?")
	}

	b.sema.Lock()
	// Can't defer b.sema.Unlock() cause of b.wg.Wait() at the end of function.

	if b.isStarted {
		b.sema.Unlock()
		return ekaerr.RejectedOperation.
			New("Bokchoy: Bokchoy already running").
			Throw()
	}

	queuesList := strings.Join(b.queueNames(), ", ")

	if b.logger.IsValid() {
		b.logger.Debug("Bokchoy: Starting queues and their consumers...",
			"bokchoy_queues_list", queuesList)
	}

	for _, queue := range b.queues {
		queue.start()
	}

	b.isStarted = true
	b.sema.Unlock()

	if b.logger.IsValid() {
		b.logger.Debug("Bokchoy: Queues and their consumers has been started.",
			"bokchoy_queues_list", queuesList)
	}

	b.wg.Wait()
	return nil
}

// Stop stops all queues and their consumers.
//
// Stopping of queues and consumers can not be failed (it's just goroutines).
// So, there is no returned error object, cause it never fail.
//
// Does nothing if Bokchoy is not running.
func (b *Bokchoy) Stop() {

	if !b.isValid() {
		return
	}

	b.sema.Lock()
	defer b.sema.Unlock()

	if !b.isStarted {
		return
	}

	queuesList := strings.Join(b.queueNames(), ", ")

	if b.logger.IsValid() {
		b.logger.Debug("Bokchoy: Stopping queues and their consumers...",
			"bokchoy_queues_list", queuesList)
	}

	for _, queue := range b.queues {
		queue.stop() // can not fail
	}

	if b.logger.IsValid() {
		b.logger.Debug("Bokchoy: Queues and their consumers has been stopped.",
			"bokchoy_queues_list", queuesList)
	}
}

// Use append a new middleware to the system.
// Does nothing if Bokchoy already running (Run() has called).
func (b *Bokchoy) Use(queueName string, handlers ...HandlerFunc) *Bokchoy {
	return b.Queue(queueName).Use(handlers...).parent
}

// Empty empties initialized queues.
// Returns an error of the first queue that can not be emptied.
// Does nothing (but returns an error) if Bokchoy already running (Run() has called).
func (b *Bokchoy) Empty() *ekaerr.Error {

	if !b.isValid() {
		return ekaerr.InitializationFailed.
			New("Bokchoy: Bokchoy is not initialized. " +
				"Did you just create an object instead of using constructor or initializer?")
	}

	b.sema.Lock()
	defer b.sema.Unlock()

	if b.isStarted {
		return ekaerr.RejectedOperation.
			New("Bokchoy: Bokchoy is running").
			Throw()
	}

	for _, queue := range b.queues {
		if err := queue.Empty(); err.IsNotNil() {
			return err.
				AddMessage("Bokchoy: Failed to empty all queues").
				Throw()
		}
	}

	return nil
}

// ClearAll clears all queues in the broker and also removes all metadata.
// Does nothing (but returns an error) if Bokchoy already running (Run() has called).
func (b *Bokchoy) ClearAll() *ekaerr.Error {

	if !b.isValid() {
		return ekaerr.InitializationFailed.
			New("Bokchoy: Bokchoy is not initialized. " +
				"Did you just create an object instead of using constructor or initializer?")
	}

	b.sema.Lock()
	defer b.sema.Unlock()

	if b.isStarted {
		return ekaerr.RejectedOperation.
			New("Bokchoy: Bokchoy is running").
			Throw()
	}

	return b.broker.ClearAll()
}

// Publish publishes a new payload to a queue.
func (b *Bokchoy) Publish(

	queueName string,
	payload   interface{},
	options   ...Option,
) (
	*Task,
	*ekaerr.Error,
) {
	if !b.isValid() {
		return nil, ekaerr.InitializationFailed.
			New("Bokchoy: Bokchoy is not initialized. " +
				"Did you just create an object instead of using constructor or initializer?")
	}

	return b.Queue(queueName).Publish(payload, options...)
}

