package femq

import (
	"sync"
)

var (
	rabbitMQ_uri string
	exchangeName string
	queueName    string
)

func initMQConfig(mq_uri string) {
	rabbitMQ_uri = mq_uri
}

func GetConnection(connection_type string, rabbitMQ_uri string) *BaseMq {
	initMQConfig(rabbitMQ_uri)
	switch connection_type {
	case "manager":
		return getManagerMq()
	case "monitor":
		return getMonitorMq()
	default:
		panic("无效运算符号")
		return nil
	}
}

var managerMq *BaseMq
var managerMqonce sync.Once
var monitorMq *BaseMq
var monitorMqonce sync.Once

func getManagerMq() *BaseMq {
	managerMqonce.Do(func() {
		managerMq = &BaseMq{
			MqConnection: &MqConnection{MqUri: rabbitMQ_uri},
		}
		managerMq.Init()
	})
	return managerMq
}

func getMonitorMq() *BaseMq {
	monitorMqonce.Do(func() {
		monitorMq = &BaseMq{
			MqConnection: &MqConnection{MqUri: rabbitMQ_uri},
		}
		monitorMq.Init()
	})
	return monitorMq
}
