package rabbit

import (
	"math"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
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

type Rabbit struct {
	context     context.Context
	authority   string
	exchange    exchange
	queue       queue
	queueBind   queueBind
	bindingKeys []string
	consume     consume
	logger      logger
}

type OptionFunc func(r *Rabbit)

func New(options ...OptionFunc) *Rabbit {
	r := &Rabbit{}

	for _, fn := range options {
		fn(r)
	}

	return r
}

func OptionContext(ctx context.Context) OptionFunc {
	return func(r *Rabbit) {
		r.context = ctx
	}
}

func OptionAuthority(authority string) OptionFunc {
	return func(r *Rabbit) {
		r.authority = authority
	}
}

func OptionLogger(logger logger) OptionFunc {
	return func(r *Rabbit) {
		r.logger = logger
	}
}

func OptionExchange(name, kind string, durable, autoDelete, internal, noWait bool) OptionFunc {
	return func(r *Rabbit) {
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

func OptionConsume(consumeTag string, noAck, exclusive, noLocal, noWait bool) OptionFunc {
	return func(r *Rabbit) {
		r.consume = consume{
			consumeTag: consumeTag,
			noAck:      noAck,
			exclusive:  exclusive,
			noLocal:    noLocal,
			noWait:     noWait,
		}
	}
}

func OptionQueue(name string, durable, autoDelete, exclusive, noWait bool) OptionFunc {
	return func(r *Rabbit) {
		r.queue = queue{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
			exclusive:  exclusive,
			noWait:     noWait,
		}
	}
}

func OptionQueueBind(noWait bool) OptionFunc {
	return func(r *Rabbit) {
		r.queueBind = queueBind{
			noWait: noWait,
		}
	}
}

func OptionBindingKeys(keys []string) OptionFunc {
	return func(r *Rabbit) {
		r.bindingKeys = keys
	}
}

func (r *Rabbit) StartListening() <-chan amqp.Delivery {
	req, res := redial(r)

	out := make(chan amqp.Delivery, 100)

	go func() {
		defer close(out)

		for {
			req <- true
			session := <-res

			queue, err := session.channel.QueueDeclare(r.queue.name, r.queue.durable, r.queue.autoDelete, r.queue.exclusive, r.queue.noWait, nil)
			if err != nil {
				r.logger.Errorf("failed to declare queue: %s", queue.Name)
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
					r.logger.Errorf("failed to bind to key: %s, error: %s", key, err)
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
				r.logger.Errorf("failed to consume from queue %s, error: %s", queue.Name, err)

				session.Close()
				continue
			}

			for d := range deliveries {
				select {
				case out <- d:
				case <-r.context.Done():
					r.logger.Infof("shutting down listener")
					session.Close()
					return

				}
			}

			session.Close()
		}
	}()

	return out
}

func redial(r *Rabbit) (request chan<- bool, response <-chan session) {
	req := make(chan bool)
	res := make(chan session)

	go func() {
		defer close(req)
		defer close(res)

		for {
			select {
			case <-req:
			case <-r.context.Done():
				r.logger.Infof("shutting down session reconnector")
				return
			}

			connect(r, res)
		}
	}()

	return req, res
}

func connect(r *Rabbit, out chan<- session) {
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
		case <-r.context.Done():
			r.logger.Infof("shutting down session reconnector.")
			return
		}

		conn, err := amqp.Dial(r.authority)
		if err != nil {
			backoff = exp(backoff)
			r.logger.Errorf("failed to connect to AMQP, will retry in: %f seconds", backoff)
			continue
		}

		channel, err := conn.Channel()
		if err != nil {
			conn.Close()
			backoff = exp(backoff)
			r.logger.Errorf("failed to get channel from AMQP, will retry in: %f seconds", backoff)
			continue
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
			r.logger.Errorf("failed to declare exchange in AMQP, will retry in: %f seconds, error: %s", backoff, err)
			continue
		}

		backoff = 0.0

		select {
		case out <- session{conn, channel}:
			r.logger.Infof("connected to AMQP")
			return
		case <-r.context.Done():
			r.logger.Infof("shutting down reconnector")
			return
		}

	}
}
