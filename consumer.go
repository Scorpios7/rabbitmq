package rabbitmq

import (
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	Option  Option
}

func NewConsumer(option Option) *Consumer {
	conn, err := amqp.Dial(option.RabbitmqUrl)
	if err != nil {
		panic(err)
	}
	if option.ConsumerWorker <= 0 {
		option.ConsumerWorker = 1
	}

	consumer := &Consumer{
		conn:   conn,
		Option: option,
	}
	err = consumer.initChannel()
	if err != nil {
		consumer.Close()
		panic(err)
	}
	return consumer
}

func (consumer *Consumer) Close() error {
	if consumer.channel != nil {
		if err := consumer.channel.Close(); err != nil {
			return err
		}
	}
	if consumer.conn != nil {
		if err := consumer.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (consumer *Consumer) initChannel() error {
	channel, err := consumer.conn.Channel()
	if err != nil {
		return err
	}
	if consumer.Option.QueueName != "" {
		_, err = queueDeclare(channel, consumer.Option)
		if err != nil {
			return err
		}
	}
	if consumer.Option.ExchangeName != "" {
		err = exchangeDeclare(channel, consumer.Option)
		if err != nil {
			return err
		}
	}
	if consumer.Option.BindingKey != "" {
		err = queueBind(channel, consumer.Option)
		if err != nil {
			return err
		}
	}

	prefetch := 1
	if consumer.Option.ConsumerPrefetch > 0 {
		prefetch = consumer.Option.ConsumerPrefetch
	}
	if err = channel.Qos(prefetch, 0, false); err != nil {
		return err
	}

	consumer.channel = channel
	return nil
}

func (consumer *Consumer) Consume(handler func(msg []byte) error) error {
	wg := sync.WaitGroup{}
	workerNum := consumer.Option.ConsumerWorker
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			err := consumer.handleMessage(index, handler)
			if err != nil {
				log.Println(err.Error())
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (consumer *Consumer) handleMessage(index int, handler func(msg []byte) error) error {
	channel := consumer.channel

	deliveries, err := channel.Consume(
		consumer.Option.QueueName,
		fmt.Sprintf("%s_%d", consumer.Option.ConsumerTag, index+1),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for d := range deliveries {
		err = handler(d.Body)
		if err != nil {
			// 消费异常, 需记录日志
			log.Println(err.Error())
			// 丢到dlx
			// TODO 引入重试机制重新入队消费, 超过一定次数后判断队列是否绑定了dlx, 若绑定了就丢到dlx, 否则就Nack
			err = d.Nack(false, false)
			if err != nil {
				return err
			}
		} else {
			err = d.Ack(false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
