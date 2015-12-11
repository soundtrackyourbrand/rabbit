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

type AMQP struct {
	context     context.Context
	authority   string
	exchange    exchange
	queue       queue
	queueBind   queueBind
	bindingKeys []string
	consume     consume
	logger      logger
}

type AMQPOptionFunc func(a *AMQP)

func NewAMQP(options ...AMQPOptionFunc) *AMQP {
	a := &AMQP{context: c}

	for _, fn := range options {
		fn(a)
	}

	return a
}

func AMQPOptionContext(ctx context.Context) AMQPOptionFunc {
	return func(a *AMQP) {
		a.context = ctx
	}
}

func AMQPOptionAuthority(authority string) AMQPOptionFunc {
	return func(a *AMQP) {
		a.authority = authority
	}
}

func AMQPOptionLogger(logger logger) AMQPOptionFunc {
	return func(a *AMQP) {
		a.logger = logger
	}
}

func AMQPOptionExchange(name, kind string, durable, autoDelete, internal, noWait bool) AMQPOptionFunc {
	return func(a *AMQP) {
		a.exchange = exchange{
			name:       name,
			kind:       kind,
			durable:    durable,
			autoDelete: autoDelete,
			internal:   internal,
			noWait:     noWait,
		}
	}
}

func AMQPOptionConsume(consumeTag string, noAck, exclusive, noLocal, noWait bool) AMQPOptionFunc {
	return func(a *AMQP) {
		a.consume = consume{
			consumeTag: consumeTag,
			noAck:      noAck,
			exclusive:  exclusive,
			noLocal:    noLocal,
			noWait:     noWait,
		}
	}
}

func AMQPOptionQueue(name string, durable, autoDelete, exclusive, noWait bool) AMQPOptionFunc {
	return func(a *AMQP) {
		a.queue = queue{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
			exclusive:  exclusive,
			noWait:     noWait,
		}
	}
}

func AMQPOptionQueueBind(noWait bool) AMQPOptionFunc {
	return func(a *AMQP) {
		a.queueBind = queueBind{
			noWait: noWait,
		}
	}
}

func AMQPOptionBindingKeys(keys []string) AMQPOptionFunc {
	return func(a *AMQP) {
		a.bindingKeys = keys
	}
}

func (a *AMQP) StartListening() <-chan amqp.Delivery {
	req, res := redial(a)

	out := make(chan amqp.Delivery, 100)

	go func() {
		defer close(out)

		for {
			req <- true
			session := <-res

			queue, err := session.channel.QueueDeclare(a.queue.name, a.queue.durable, a.queue.autoDelete, a.queue.exclusive, a.queue.noWait, nil)
			if err != nil {
				a.logger.Errorf("failed to declare queue: %s", queue.Name)
				session.Close()
				continue
			}

			for _, key := range a.bindingKeys {
				err = session.channel.QueueBind(
					a.queue.name,
					key,
					a.exchange.name,
					a.queueBind.noWait,
					nil,
				)
				if err != nil {
					a.logger.Errorf("failed to bind to key: %s, error: %s", key, err)
					session.Close()
					continue
				}
			}

			deliveries, err := session.channel.Consume(
				a.queue.name,
				a.consume.consumeTag,
				a.consume.noAck,
				a.consume.exclusive,
				a.consume.noLocal,
				a.consume.noWait,
				nil,
			)
			if err != nil {
				a.logger.Errorf("failed to consume from queue %s, error: %s", queue.Name, err)

				session.Close()
				continue
			}

			for d := range deliveries {
				out <- d
			}

			session.Close()
		}
	}()

	return out
}

func redial(a *AMQP) (request chan<- bool, response <-chan session) {
	req := make(chan bool)
	res := make(chan session)

	go func() {
		defer close(req)
		defer close(res)

		for {
			select {
			case <-req:
			case <-a.context.Done():
				a.logger.Infof("shutting down session reconnector")
				return
			}

			connect(a, res)
		}
	}()

	return req, res
}

func connect(a *AMQP, out chan<- session) {
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
		case <-a.context.Done():
			a.logger.Infof("shutting down session reconnector.")
			return
		}

		conn, err := amqp.Dial(a.authority)
		if err != nil {
			backoff = exp(backoff)
			a.logger.Errorf("failed to connect to AMQP, will retry in: %f seconds", backoff)
			continue
		}

		channel, err := conn.Channel()
		if err != nil {
			conn.Close()
			backoff = exp(backoff)
			a.logger.Errorf("failed to get channel from AMQP, will retry in: %f seconds", backoff)
			continue
		}

		err = channel.ExchangeDeclare(
			a.exchange.name,
			a.exchange.kind,
			a.exchange.durable,
			a.exchange.autoDelete,
			a.exchange.internal,
			a.exchange.noWait,
			nil,
		)
		if err != nil {
			channel.Close()
			conn.Close()
			backoff = exp(backoff)
			a.logger.Errorf("failed to declare exchange in AMQP, will retry in: %f seconds, error: %s", backoff, err)
			continue
		}

		backoff = 0.0

		select {
		case out <- session{conn, channel}:
			a.logger.Infof("connected to AMQP")
			return
		case <-a.context.Done():
			a.logger.Infof("shutting down reconnector")
			return
		}

	}
}
