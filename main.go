package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// --- CONFIGURATION ---
const (
	MQTT_BROKER    = "tcp://broker.hivemq.com:1883"
	MQTT_TOPIC     = "brake/temperature" // + is a wildcard matching any device ID
	MQTT_CLIENT_ID = "go_mqtt_forwarder"

	INFLUX_URL    = "http://43.159.61.247:8086"
	INFLUX_TOKEN  = "GNZfJK6tIsvZncMDa8lqRk1N0N2EorM-SXuYD7P311aB1_7uAapSEXZ3Bb04eIy7q63rEFO1_ZuT1LnRSbKHzQ==" // Leave empty if InfluxDB v1 without auth
	INFLUX_ORG    = "ae2fffe86c7b4667"
	INFLUX_BUCKET = "brake-sensor"
)

// Define a structured message for our internal Go channel
type SensorPayload struct {
	Suhu   float64 `json:"suhu"`
	Status string  `json:"status"`
	Unit   string  `json:"unit"`
}

// Structured message for internal Go channel queue
type SensorData struct {
	DeviceID  string
	Payload   SensorPayload
	Timestamp time.Time
}

func main() {
	log.Println("🚀 Starting JSON-compatible Go MQTT to InfluxDB Forwarder...")

	// 2. Initialize InfluxDB Client
	influxClient := influxdb2.NewClient(INFLUX_URL, INFLUX_TOKEN)
	defer influxClient.Close()

	writeAPI := influxClient.WriteAPI(INFLUX_ORG, INFLUX_BUCKET)

	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			log.Printf("❌ InfluxDB Write Error: %s\n", err.Error())
		}
	}()

	// 3. Create high-speed data channel
	dataChannel := make(chan SensorData, 1000)
	go influxWorker(writeAPI, dataChannel)

	// 4. Update the MQTT Message Handler to parse JSON
	var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		var payload SensorPayload

		// Parse the incoming JSON buffer directly into our Go struct
		err := json.Unmarshal(msg.Payload(), &payload)
		if err != nil {
			log.Printf("⚠️ Warning: Failed to parse JSON payload on topic %s: %s\n", msg.Topic(), string(msg.Payload()))
			return
		}

		// Extract device ID from topic string (e.g., "home/sensors/kitchen" -> "kitchen")
		topicParts := strings.Split(msg.Topic(), "/")
		deviceID := topicParts[len(topicParts)-1]

		// Drop the entire struct package onto the queue channel
		dataChannel <- SensorData{
			DeviceID:  deviceID,
			Payload:   payload,
			Timestamp: time.Now(),
		}
	}

	// 5. Configure and Connect MQTT
	opts := mqtt.NewClientOptions()
	opts.AddBroker(MQTT_BROKER)
	opts.SetClientID(MQTT_CLIENT_ID)
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("✅ Connected to MQTT Broker!")
		if token := client.Subscribe(MQTT_TOPIC, 0, nil); token.Wait() && token.Error() != nil {
			log.Fatalf("❌ Subscription error: %s\n", token.Error())
		}
		log.Printf("📡 Subscribed to topic pattern: %s\n", MQTT_TOPIC)
	})

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ Error connecting to MQTT: %s\n", token.Error())
	}

	// Clean exit hooks
	keepAliveSignal := make(chan os.Signal, 1)
	signal.Notify(keepAliveSignal, os.Interrupt, syscall.SIGTERM)
	<-keepAliveSignal

	log.Println("\nShutting down gracefully...")
	mqttClient.Disconnect(250)
	close(dataChannel)
	writeAPI.Flush()
	log.Println("👋 Done. All records saved.")
}

// 6. Updated Worker to write rich fields and tags to InfluxDB
func influxWorker(writeAPI api.WriteAPI, dataCh chan SensorData) {
	for data := range dataCh {

		// InfluxDB allows storing multiple related fields together
		tags := map[string]string{
			"device_id": data.DeviceID,
			"unit":      data.Payload.Unit, // Storing unit as indexed metadata
		}

		fields := map[string]interface{}{
			"suhu":   data.Payload.Suhu,   // Numeric target for charts
			"status": data.Payload.Status, // String attribute tracking warnings
		}

		point := influxdb2.NewPoint(
			"sensor_logs",
			tags,
			fields,
			data.Timestamp,
		)

		writeAPI.WritePoint(point)
		log.Printf("💾 Forwarded -> Device: %s | Suhu: %.2f%s | Status: %s\n",
			data.DeviceID, data.Payload.Suhu, data.Payload.Unit, data.Payload.Status)
	}
}
