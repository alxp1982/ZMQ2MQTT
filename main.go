package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	zmq "github.com/pebbe/zmq4"
	"github.com/tkanos/gonfig"
	"golang.org/x/exp/slices"
)

func forwarder_thread(logger *zap.SugaredLogger, config *Configuration, cancel chan bool) {
	logger.Debugln("enter forward_thread")
	defer logger.Debugln("exit forward_thread")

	pipe, _ := zmq.NewSocket(zmq.PAIR)
	err := pipe.Bind("inproc://pipe")

	if err != nil {
		logger.Error(err)
		//communicate error to main thread
		return
	}

	defer pipe.Close()

	//setup mqtt connection
	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID(config.MqttClientId)

	opts.SetKeepAlive(time.Duration(config.MqttConfig.KeepAliveTimeout * int(time.Second)))
	opts.SetPingTimeout(time.Duration(config.MqttConfig.PingTimeout * int(time.Second)))
	opts.SetMaxReconnectInterval(time.Duration(config.MqttConfig.MaxReconnectInterval * int(time.Second)))
	opts.AutoReconnect = config.MqttConfig.AutoReconnect

	//MQTT handlers
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		logger.Warnf("!!!!!! mqtt connection lost error: %s\n", err.Error())
	})

	opts.SetReconnectingHandler(func(c mqtt.Client, options *mqtt.ClientOptions) {
		logger.Infoln("mqtt reconnecting.....")
	})

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		logger.Infoln("mqtt connected.....")
	})

	c := mqtt.NewClient(opts)

	//try connecting until succesfull
	for token := c.Connect(); token.Wait() && token.Error() != nil; {
		logger.Warnf("failed to connect ot MQTT. Error:%v. Retrying in %d sec", token.Error(), 10)
		time.Sleep(10 * time.Second)
	}

	defer c.Disconnect(200)

	//setup pooler for pipe socket
	poller := zmq.NewPoller()
	poller.Add(pipe, zmq.POLLIN)

	for {
		//pool for messages
		if sockets, err := poller.Poll(100 * time.Millisecond); err == nil && len(sockets) > 0 {

			if !c.IsConnected() {
				logger.Warnln("MQTT not connected. Skipping message")
				continue
			}

			for _, socket := range sockets {
				if socket.Events&zmq.POLLIN != 0 {
					//receive messages from ZMQ with timeout
					msgs, err := pipe.RecvMessage(0)

					if err != nil {
						logger.Errorln(err)
						continue
					}

					logger.Debugf("Received messages %d", len(msgs))

					for _, msg := range msgs {
						topic := strings.Split(msg, " ")[0]

						if len(msg) > len(topic) {
							payload := msg[len(topic):]

							if slices.IndexFunc(config.ForwardTopics, func(s string) bool {
								return s == topic
							}) != -1 {
								logger.Debugf("Forwarding message %s", payload)
								//send message to MQTT topic
								token := c.Publish(topic, 1, false, payload)

								if token.Error() != nil {
									logger.Warnln(token.Error().Error())
								}
							}
						}
					}
				}
			}
		}

		// exit gorouting when signaled
		select {
		case <-cancel:
			return
		default:
		}
	}

}

func main() {
	//load configuration
	configuration := Configuration{}
	err := gonfig.GetConf(getConfigFileName(), &configuration)

	if err != nil {
		panic(err)
	}

	logger := zap.Must(configuration.LoggerConfig.Build())
	defer logger.Sync()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	sugar := logger.Sugar()

	sugar.Infoln("Command line arguments:")
	sugar.Infof("Front-end port: %d\n", configuration.FrontendPort)
	sugar.Infof("Back-end port: %d\n", configuration.BackendPort)
	sugar.Infof("MQTT Server: %s\n", configuration.MqttServer)
	sugar.Infof("Topics:%v\n", configuration.ForwardTopics)

	cancel := make(chan bool)

	go forwarder_thread(sugar, &configuration, cancel)
	defer func() { cancel <- true }()

	//Setup proxy
	subscriber, _ := zmq.NewSocket(zmq.XSUB)
	subscriber_addr := fmt.Sprintf("tcp://*:%d", configuration.FrontendPort)
	subscriber.Bind(subscriber_addr)

	publisher, _ := zmq.NewSocket(zmq.XPUB)
	publisher.Bind(fmt.Sprintf("tcp://*:%d", configuration.BackendPort))

	listener, _ := zmq.NewSocket(zmq.PAIR)
	listener.Connect("inproc://pipe")

	zmq.Proxy(subscriber, publisher, listener)
}

func getConfigFileName() string {
	env := os.Getenv("ENV")
	if len(env) == 0 {
		env = "development"
	}
	filename := []string{"config/", "config.", env, ".json"}
	_, dirname, _, _ := runtime.Caller(0)
	filePath := path.Join(filepath.Dir(dirname), strings.Join(filename, ""))

	return filePath
}
