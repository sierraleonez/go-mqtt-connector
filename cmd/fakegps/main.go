package main

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	opts := mqtt.NewClientOptions().AddBroker("tcp://43.159.61.247:1883").SetClientID("fake_gps_pub")
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	msg := fmt.Sprintf(`{"lat":-6.200000,"lng":106.816666,"speed":45.2,"altitude":15.3,"time":"%s","satellites":8}`, time.Now().Format(time.RFC3339))
	token := client.Publish("brake/gps", 0, false, msg)
	token.Wait()
	fmt.Println("✅ Published to brake/gps:", msg)
	client.Disconnect(250)
}
