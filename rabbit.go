package rabbit

import (
	"math"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type queue struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
}

type queueBind struct {
	noWait bool
}

type exchange struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
}

type consume struct {
	consumeTag string
	noAck      bool
	exclusive  bool
	noLocal    bool
	noWait     bool
}

type session struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (s *session) Close() {
	if s.channel != nil {
		_ = s.channel.Close()
	}

	if s.connection != nil {
		_ = s.connection.Close()
	}
}

type Rabbit interface {
	StartListening() <-chan amqp.Delivery
	StartPublishing() PublishingChannel
}

var _ Rabbit = (*RabbitImpl)(nil)

type RabbitImpl struct {
	authority     string
	exchange      exchange
	queue         queue
	queueBind     queueBind
	bindingKeys   []string
	prefetchCount int
	consume       consume

	Context context.Context `inject:""`
	Logger  logger          `inject:""`
}

type OptionFunc func(r *RabbitImpl)

func New(options ...OptionFunc) Rabbit {
	r := &RabbitImpl{}

	for _, fn := range options {
		fn(r)
	}

	return r
}

func WithContext(context context.Context) OptionFunc {
	return func(r *RabbitImpl) {
		r.Context = context
	}
}

func WithAuthority(authority string) OptionFunc {
	return func(r *RabbitImpl) {
		r.authority = authority
	}
}

func WithLogger(logger logger) OptionFunc {
	return func(r *RabbitImpl) {
		r.Logger = logger
	}
}

func WithExchange(name, kind string, durable, autoDelete, internal, noWait bool) OptionFunc {
	return func(r *RabbitImpl) {
		r.exchange = exchange{
			name:       name,
			kind:       kind,
			durable:    durable,
			autoDelete: autoDelete,
			internal:   internal,
			noWait:     noWait,
		}
	}
}

func WithConsume(consumeTag string, noAck, exclusive, noLocal, noWait bool) OptionFunc {
	return func(r *RabbitImpl) {
		r.consume = consume{
			consumeTag: consumeTag,
			noAck:      noAck,
			exclusive:  exclusive,
			noLocal:    noLocal,
			noWait:     noWait,
		}
	}
}

func WithQueue(name string, durable, autoDelete, exclusive, noWait bool) OptionFunc {
	return func(r *RabbitImpl) {
		r.queue = queue{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
			exclusive:  exclusive,
			noWait:     noWait,
		}
	}
}

func WithQueueBind(noWait bool) OptionFunc {
	return func(r *RabbitImpl) {
		r.queueBind = queueBind{
			noWait: noWait,
		}
	}
}

func WithBindingKeys(keys []string) OptionFunc {
	return func(r *RabbitImpl) {
		r.bindingKeys = keys
	}
}

func WithQos(prefetchCount int) OptionFunc {
	return func(r *RabbitImpl) {
		r.prefetchCount = prefetchCount
	}
}

type publishingRequest struct {
	Publishing amqp.Publishing
	Exchange   string
	RoutingKey string
	reply      chan error
}

type PublishingChannel chan publishingRequest

func (pc PublishingChannel) Publish(exchange, routingKey string, msg amqp.Publishing) error {
	req := publishingRequest{
		Publishing: msg,
		Exchange:   exchange,
		RoutingKey: routingKey,
		reply:      make(chan error),
	}

	pc <- req

	return <-req.reply
}

func (r *RabbitImpl) StartPublishing() PublishingChannel {
	req, res := redial(r)

	out := make(PublishingChannel)

	go func() {
		defer close(out)

		for {
			req <- true
			session := <-res

			for msg := range out {
				if err := session.channel.Publish(msg.Exchange, msg.RoutingKey, false, false, msg.Publishing); err != nil {
					msg.reply <- err
				} else {
					close(msg.reply)
				}
			}

			r.Logger.Infof("shutting down publisher")
			session.Close()
		}
	}()

	return out
}

func (r *RabbitImpl) StartListening() <-chan amqp.Delivery {
	req, res := redial(r)

	out := make(chan amqp.Delivery, 100)

	go func() {
		defer close(out)

		for {
			req <- true
			session := <-res

			queue, err := session.channel.QueueDeclare(r.queue.name, r.queue.durable, r.queue.autoDelete, r.queue.exclusive, r.queue.noWait, nil)
			if err != nil {
				r.Logger.Errorf("failed to declare queue '%s' durable(%t) autoDelete(%t) exclusive(%t) noWait(%t)", r.queue.name, r.queue.durable, r.queue.autoDelete, r.queue.exclusive, r.queue.noWait)
				session.Close()
				continue
			}

			for _, key := range r.bindingKeys {
				err = session.channel.QueueBind(
					r.queue.name,
					key,
					r.exchange.name,
					r.queueBind.noWait,
					nil,
				)
				if err != nil {
					r.Logger.Errorf("failed to bind to key: %s, error: %s", key, err)
					session.Close()
					continue
				}
			}

			deliveries, err := session.channel.Consume(
				r.queue.name,
				r.consume.consumeTag,
				r.consume.noAck,
				r.consume.exclusive,
				r.consume.noLocal,
				r.consume.noWait,
				nil,
			)
			if err != nil {
				r.Logger.Errorf("failed to consume from queue %s, error: %s", queue.Name, err)

				session.Close()
				continue
			}

			for d := range deliveries {
				select {
				case out <- d:
				case <-r.Context.Done():
					r.Logger.Infof("shutting down listener")
					session.Close()
					return

				}
			}

			session.Close()
		}
	}()

	return out
}

func redial(r *RabbitImpl) (request chan<- bool, response <-chan session) {
	req := make(chan bool)
	res := make(chan session)

	go func() {
		defer close(req)
		defer close(res)

		for {
			select {
			case <-req:
			case <-r.Context.Done():
				r.Logger.Infof("shutting down session reconnector")
				return
			}

			connect(r, res)
		}
	}()

	return req, res
}

func connect(r *RabbitImpl, out chan<- session) {
	backoff := 0.0

	exp := func(b float64) float64 {
		if b == 0.0 {
			return 0.25
		}

		return math.Min(float64(b*2), 30)
	}

	for {
		select {
		case <-time.After(time.Duration(backoff) * time.Second):
		case <-r.Context.Done():
			r.Logger.Infof("shutting down session reconnector.")
			return
		}

		conn, err := amqp.Dial(r.authority)
		if err != nil {
			backoff = exp(backoff)
			r.Logger.Errorf("failed to connect to AMQP, will retry in: %f seconds", backoff)
			continue
		}

		channel, err := conn.Channel()
		if err != nil {
			conn.Close()
			backoff = exp(backoff)
			r.Logger.Errorf("failed to get channel from AMQP, will retry in: %f seconds", backoff)
			continue
		}

		if r.prefetchCount > 0 {
			err = channel.Qos(r.prefetchCount, 0, true)

			if err != nil {
				channel.Close()
				conn.Close()
				backoff = exp(backoff)
				r.Logger.Errorf("failed to set channel Qos, will retry in: %f seconds", backoff)
				continue
			}
		}

		err = channel.ExchangeDeclare(
			r.exchange.name,
			r.exchange.kind,
			r.exchange.durable,
			r.exchange.autoDelete,
			r.exchange.internal,
			r.exchange.noWait,
			nil,
		)

		if err != nil {
			channel.Close()
			conn.Close()
			backoff = exp(backoff)
			r.Logger.Errorf("failed to declare exchange in AMQP, will retry in: %f seconds, error: %s", backoff, err)
			continue
		}

		backoff = 0.0

		select {
		case out <- session{conn, channel}:
			r.Logger.Infof("connected to AMQP")
			return
		case <-r.Context.Done():
			r.Logger.Infof("shutting down reconnector")
			return
		}
	}
}
