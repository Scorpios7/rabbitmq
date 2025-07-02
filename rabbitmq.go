package rabbitmq

import (
	"github.com/pkg/errors"

	"github.com/streadway/amqp"
)

const (
	ExchangeDirect = "direct"
	ExchangeTopic  = "topic"
	ExchangeFanout = "fanout"
)

type Option struct {
	RabbitmqUrl      string
	ExchangeName     string
	ExchangeType     string
	RoutingKey       string
	QueueName        string
	BindingKey       string
	ConsumerTag      string
	ConsumerWorker   int
	ConsumerPrefetch int
	Dlx              string // 死信交换机
	DlxRoutingKey    string
}

func exchangeDeclare(channel *amqp.Channel, option Option) error {
	if option.ExchangeName == "" {
		return errors.New("Exchange name is empty")
	}
	isValidExchangeType := false
	validExchangeTypes := []string{ExchangeDirect, ExchangeFanout, ExchangeTopic}
	for _, t := range validExchangeTypes {
		if option.ExchangeType == t {
			isValidExchangeType = true
		}
	}
	if !isValidExchangeType {
		return errors.New("Invalid exchange type")
	}

	return channel.ExchangeDeclare(
		option.ExchangeName,
		option.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
}

func queueDeclare(channel *amqp.Channel, option Option) (amqp.Queue, error) {
	if option.QueueName == "" {
		return amqp.Queue{}, errors.New("queue name is empty")
	}

	args := amqp.Table{}
	if option.Dlx != "" && option.DlxRoutingKey != "" {
		args = amqp.Table{
			"x-dead-letter-exchange":    option.Dlx, // 死信交换机
			"x-dead-letter-routing-key": option.DlxRoutingKey,
		}
	}
	return channel.QueueDeclare(
		option.QueueName,
		true,
		false,
		false,
		false,
		args,
	)
}

func queueBind(channel *amqp.Channel, option Option) error {
	if option.QueueName == "" {
		return errors.New("Queue name is empty")
	}
	if option.ExchangeName == "" {
		return errors.New("Exchange name is empty")
	}
	if option.BindingKey == "" {
		return errors.New("Binding key is empty")
	}

	return channel.QueueBind(
		option.QueueName,
		option.BindingKey,
		option.ExchangeName,
		false,
		nil,
	)
}
