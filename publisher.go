package rabbitmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	Option  Option
}

func NewPublisher(option Option) *Publisher {
	conn, err := amqp.Dial(option.RabbitmqUrl)
	if err != nil {
		panic(err)
	}

	publisher := &Publisher{
		conn:   conn,
		Option: option,
	}
	err = publisher.initChannel()
	if err != nil {
		panic(err)
	}
	return publisher
}

func (publisher *Publisher) Close() error {
	if publisher.channel != nil {
		if err := publisher.channel.Close(); err != nil {
			return err
		}
	}
	if publisher.conn != nil {
		if err := publisher.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (publisher *Publisher) initChannel() error {
	channel, err := publisher.conn.Channel()
	if err != nil {
		return err
	}
	if publisher.Option.ExchangeName != "" {
		err = exchangeDeclare(channel, publisher.Option)
		if err != nil {
			return err
		}
	}
	if publisher.Option.QueueName != "" {
		_, err = queueDeclare(channel, publisher.Option)
		if err != nil {
			return err
		}
	}
	if publisher.Option.BindingKey != "" {
		err = queueBind(channel, publisher.Option)
		if err != nil {
			return err
		}
	}

	publisher.channel = channel
	return nil
}

func (publisher *Publisher) PublishToExchange(message string) error {
	if publisher.Option.ExchangeName == "" {
		return errors.New("Exchange name is empty")
	}
	if publisher.Option.RoutingKey == "" {
		return errors.New("routing key is empty")
	}

	err := publisher.channel.Publish(
		publisher.Option.ExchangeName,
		publisher.Option.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	if err != nil {
		return err
	}
	return nil
}
