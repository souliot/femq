package femq

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/astaxie/beego"
	"github.com/streadway/amqp"
)

// type SiDelivery amqp.Delivery

type MqConnection struct {
	Lock       sync.RWMutex
	Connection *amqp.Connection
	MqUri      string
}

type ChannelContext struct {
	Exchange     string
	ExchangeType string
	RoutingKey   string
	Reliable     bool
	Durable      bool
	ChannelId    string
	Channel      *amqp.Channel
}
type BaseMq struct {
	MqConnection *MqConnection
	//channel cache
	ChannelContexts map[string]*ChannelContext
}

func (bmq *BaseMq) Init() {
	bmq.ChannelContexts = make(map[string]*ChannelContext)
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func (bmq *BaseMq) confirmOne(confirms <-chan amqp.Confirmation) {
	beego.Info("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		beego.Info("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		beego.Error("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

/*
func (bmq *BaseMq) getMqUri() string {
	return "amqp://" + bmq.MqConnection.User + ":" + bmq.MqConnection.PassWord + "@" + bmq.MqConnection.Host + ":" + bmq.MqConnection.Port + "/"
}
*/
/*
get md5 from channel context
*/
func (bmq *BaseMq) generateChannelId(channelContext *ChannelContext) string {
	stringTag := channelContext.Exchange + ":" + channelContext.ExchangeType + ":" + channelContext.RoutingKey + ":" +
		strconv.FormatBool(channelContext.Durable) + ":" + strconv.FormatBool(channelContext.Reliable)
	hasher := md5.New()
	hasher.Write([]byte(stringTag))
	return hex.EncodeToString(hasher.Sum(nil))
}

/*
1. use old connection to generate channel
2. update connection then channel
*/
func (bmq *BaseMq) refreshConnectionAndChannel(channelContext *ChannelContext) error {
	bmq.MqConnection.Lock.Lock()
	defer bmq.MqConnection.Lock.Unlock()
	var err error

	if bmq.MqConnection.Connection != nil {
		channelContext.Channel, err = bmq.MqConnection.Connection.Channel()
	} else {
		beego.Info("connection not init,dial first time..")
		err = errors.New("connection nil")
	}

	// reconnect connection
	if err != nil {
		for {
			bmq.MqConnection.Connection, err = amqp.Dial(bmq.MqConnection.MqUri)
			if err != nil {
				beego.Info("connect mq get connection error,retry..." + bmq.MqConnection.MqUri)
				time.Sleep(10 * time.Second)
			} else {
				channelContext.Channel, _ = bmq.MqConnection.Connection.Channel()
				break

			}
		}
	}

	if err = channelContext.Channel.ExchangeDeclare(
		channelContext.Exchange,     // name
		channelContext.ExchangeType, // type
		channelContext.Durable,      // durable
		false,                       // auto-deleted
		false,                       // internal
		false,                       // noWait
		nil,                         // arguments
	); err != nil {
		beego.Info("channel exchange deflare failed refreshConnectionAndChannel again", err)
		return err
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.

	/*if channelContext.Reliable {
		fmt.Println("enabling publishing confirms.")
		if err := channelContext.Channel.Confirm(false); err != nil {
			fmt.Println("Channel could not be put into confirm mode: %s", err)
			return err
		}
		fmt.Println("confirm begin")
		confirms := channelContext.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		fmt.Println("confirm end")
		defer bmq.confirmOne(confirms)
	}*/

	//add channel to channel cache
	bmq.ChannelContexts[channelContext.ChannelId] = channelContext
	return nil
}

/*
publish message
*/
func (bmq *BaseMq) Publish(channelContext *ChannelContext, body string) error {
	channelContext.ChannelId = bmq.generateChannelId(channelContext)
	if bmq.ChannelContexts[channelContext.ChannelId] == nil {
		bmq.refreshConnectionAndChannel(channelContext)
	} else {
		channelContext = bmq.ChannelContexts[channelContext.ChannelId]
	}
	if err := channelContext.Channel.Publish(
		channelContext.Exchange,   // publish to an exchange
		channelContext.RoutingKey, // routing to 0 or more queues
		false,                     // mandatory
		false,                     // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		beego.Info("send message failed refresh connection")
		time.Sleep(10 * time.Second)
		bmq.refreshConnectionAndChannel(channelContext)
	} else {
		beego.Info("send messsage success", body)
	}
	return nil
}

/*
consumer message
*/
func (bmq *BaseMq) Consumer(channelContext *ChannelContext, calllback func(string) bool) error {
	channelContext.ChannelId = bmq.generateChannelId(channelContext)
	if bmq.ChannelContexts[channelContext.ChannelId] == nil {
		bmq.refreshConnectionAndChannel(channelContext)
	} else {
		channelContext = bmq.ChannelContexts[channelContext.ChannelId]
	}
	_, err := channelContext.Channel.QueueDeclare(channelContext.RoutingKey, true, false, false, false, nil)
	if err != nil {
		beego.Info("queue.declare: ", err)
	}

	err = channelContext.Channel.QueueBind(channelContext.RoutingKey, channelContext.RoutingKey, channelContext.Exchange, false, nil)
	if err != nil {
		beego.Info("queue.bind: ", err)
	}

	if msgs, err := channelContext.Channel.Consume(
		channelContext.RoutingKey, // routing to 0 or more queues
		"",                        // consumer
		false,                     // auto-ack
		false,                     // exclusive
		false,                     // no-local
		false,                     // no-wait
		nil,                       // args
	); err != nil {
		beego.Info(err)
		beego.Info("consumer message failed refresh connection")
		time.Sleep(10 * time.Second)
		bmq.refreshConnectionAndChannel(channelContext)
	} else {
		//创建一个channel
		forever := make(chan bool)

		//调用gorountine
		go func() {
			for d := range msgs {
				result := calllback(string(d.Body))
				if result {
					d.Ack(false)
				} else {
					d.Nack(false, true)
				}
			}
		}()
		<-forever
	}
	return nil
}
