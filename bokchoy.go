package bokchoy

import (
	"context"
	"strings"
	"sync"

	"github.com/thoas/bokchoy/logging"

	"github.com/pkg/errors"
)

// Bokchoy is the main object which stores all configuration, queues
// and broker.
type Bokchoy struct {
	logger         logging.Logger
	tracer         Tracer
	serializer     Serializer
	cfg            Config
	queue          *Queue
	wg             *sync.WaitGroup
	defaultOptions *Options
	broker         Broker
	queues         map[string]*Queue
	middlewares    []func(Handler) Handler
}

// New initializes a new Bokchoy instance.
func New(ctx context.Context, cfg Config, options ...Option) (*Bokchoy, error) {
	opts := newOptions()
	for i := range options {
		options[i](opts)
	}

	var (
		err    error
		tracer Tracer
	)

	logger := logging.NewNopLogger()
	if opts.Logger != nil {
		logger = opts.Logger
	}

	if opts.Tracer == nil {
		tracer = NewTracerLogger(logger)
	}

	bok := &Bokchoy{
		cfg:            cfg,
		serializer:     newSerializer(cfg.Serializer),
		queues:         make(map[string]*Queue),
		wg:             &sync.WaitGroup{},
		logger:         logger,
		tracer:         tracer,
		defaultOptions: opts,
	}

	if opts.Serializer != nil {
		bok.serializer = opts.Serializer
	}

	bok.broker = newBroker(ctx, cfg.Broker,
		logger.With(logging.String("component", "broker")))

	if opts.Initialize {
		err = bok.broker.Initialize(ctx)
		if err != nil {
			if err != nil {
				return nil, errors.Wrap(err, "unable to initialize broker")
			}
		}
	}

	for i := range cfg.Queues {
		bok.Queue(cfg.Queues[i].Name)
	}

	return bok, nil
}

// Use append a new middleware to the system.
func (b *Bokchoy) Use(sub ...func(Handler) Handler) *Bokchoy {
	b.middlewares = append(b.middlewares, sub...)

	return b
}

// Empty empties initialized queues.
func (b *Bokchoy) Empty(ctx context.Context) error {
	for i := range b.queues {
		err := b.queues[i].Empty(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Flush flushes data of the entire system.
func (b *Bokchoy) Flush() error {
	return b.broker.Flush()
}

// Queue gets or creates a new queue.
func (b *Bokchoy) Queue(name string) *Queue {
	queue, ok := b.queues[name]
	if !ok {
		queue = &Queue{
			name:           name,
			broker:         b.broker,
			serializer:     b.serializer,
			logger:         b.logger.With(logging.String("component", "queue")),
			tracer:         b.tracer,
			wg:             b.wg,
			defaultOptions: b.defaultOptions,
			middlewares:    b.middlewares,
		}

		b.queues[name] = queue
	}

	return queue
}

// Stop stops all queues and consumers.
func (b *Bokchoy) Stop(ctx context.Context) {
	fields := []logging.Field{
		logging.String("queues", strings.Join(b.QueueNames(), ",")),
	}

	b.logger.Debug(ctx, "Stopping queues...", fields...)

	for i := range b.queues {
		b.queues[i].stop(ctx)
	}

	b.logger.Debug(ctx, "Queues stopped", fields...)
}

// QueueNames returns the managed queue names.
func (b *Bokchoy) QueueNames() []string {
	names := make([]string, 0, len(b.queues))

	for k := range b.queues {
		names = append(names, k)
	}

	return names
}

// Run runs the system and block the current goroutine.
func (b *Bokchoy) Run(ctx context.Context) error {
	err := b.broker.Ping()
	if err != nil {
		return err
	}

	fields := []logging.Field{
		logging.String("queues", strings.Join(b.QueueNames(), ",")),
	}

	b.logger.Debug(ctx, "Starting queues...", fields...)

	for i := range b.queues {
		b.queues[i].start(ctx)
	}

	b.logger.Debug(ctx, "Queues started", fields...)

	b.wg.Wait()

	return nil
}

// Publish publishes a new payload to a queue.
func (b *Bokchoy) Publish(ctx context.Context, queueName string, payload interface{}, options ...Option) (*Task, error) {
	return b.Queue(queueName).Publish(ctx, payload, options...)
}

// Handle registers a new handler to consume tasks for a queue.
func (b *Bokchoy) Handle(queueName string, sub Handler, options ...Option) {
	b.HandleFunc(queueName, sub.Handle)
}

// HandleFunc registers a new handler function to consume tasks for a queue.
func (b *Bokchoy) HandleFunc(queueName string, f HandlerFunc, options ...Option) {
	b.Queue(queueName).HandleFunc(f, options...)
}
