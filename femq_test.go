package femq

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"

	"github.com/astaxie/beego"
	"github.com/souliot/femq"
)

func stringsToJSON(str string) string {
	var jsons bytes.Buffer
	for _, r := range str {
		rint := int(r)
		if rint < 128 {
			jsons.WriteRune(r)
		} else {
			jsons.WriteString("\\u")
			jsons.WriteString(strconv.FormatInt(int64(rint), 16))
		}
	}
	beego.Info(jsons.String())
	return jsons.String()
}

func Round(f float64, n int) float64 {
	pow10_n := math.Pow10(n)
	return math.Trunc((f+0.5/pow10_n)*pow10_n) / pow10_n
}

func PublishRegisterValue(data string) {
	var mq1 *femq.BaseMq
	mq1 = femq.GetConnection("manager", "mq_uri")
	channleContxt := femq.ChannelContext{
		Exchange:     "zqjl_up",
		ExchangeType: "direct",
		RoutingKey:   "zqjl_up",
		Reliable:     true,
		Durable:      true,
	}
	mq1.Publish(&channleContxt, data)
}

// 订阅发送消息
func ConsumerRegisterValue() {
	var mq1 *femq.BaseMq
	mq1 = femq.GetConnection("manager", mq_uri)
	channleContxt := femq.ChannelContext{
		Exchange:     "zqjl_down",
		ExchangeType: "direct",
		RoutingKey:   "zqjl_down",
		Reliable:     true,
		Durable:      true,
	}
	mq1.Consumer(&channleContxt, OnMessage)
}

// 下发指令处理操作
func OnMessage(msg string) bool {
	beego.Info("接收--" + msg)
	return true
}
