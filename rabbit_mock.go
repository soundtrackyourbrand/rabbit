package rabbit

import "github.com/streadway/amqp"

type Mock struct{}

var _ Rabbit = (*Mock)(nil)

func NewMock() Rabbit {
	return &Mock{}
}

func (m *Mock) StartPublishing() PublishingChannel {
	return make(PublishingChannel)
}

func (m *Mock) StartListening() <-chan amqp.Delivery {
	return make(chan amqp.Delivery)
}
