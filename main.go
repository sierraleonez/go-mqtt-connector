package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/joho/godotenv"
)

//go:embed templates/*
var templates embed.FS

//go:embed static/*
var static embed.FS

var (
	MQTT_BROKER    string
	MQTT_TOPIC     string
	MQTT_CLIENT_ID string
	INFLUX_URL     string
	INFLUX_TOKEN   string
	INFLUX_ORG     string
	INFLUX_BUCKET  string
	HTTP_PORT      string
	TELEGRAM_BOT_TOKEN string
)

type SensorPayload struct {
	Suhu   float64 `json:"suhu"`
	Status string  `json:"status"`
	Unit   string  `json:"unit"`
}

type SensorData struct {
	DeviceID  string
	Payload   SensorPayload
	Timestamp time.Time
}

type CurrentState struct {
	DeviceID string  `json:"device_id"`
	Suhu     float64 `json:"suhu"`
	Unit     string  `json:"unit"`
	Status   string  `json:"status"`
	Time     string  `json:"time"`
}

type HistoryPoint struct {
	Time  string   `json:"time"`
	Value *float64 `json:"value"`
}

type LogEntry struct {
	Time     string   `json:"time"`
	DeviceID string   `json:"device_id"`
	Suhu     *float64 `json:"suhu"`
	Status   string   `json:"status"`
	Unit     string   `json:"unit"`
}

type DashboardData struct {
	Current *CurrentState  `json:"current"`
	History []HistoryPoint `json:"history"`
	Logs    []LogEntry     `json:"logs"`
	Range   string         `json:"range"`
}

var influxClient influxdb2.Client

func main() {
	godotenv.Load()
	loadEnv()

	log.Println("🚀 Starting MQTT Forwarder + Dashboard Server...")

	influxClient = influxdb2.NewClient(INFLUX_URL, INFLUX_TOKEN)
	defer influxClient.Close()

	writeAPI := influxClient.WriteAPI(INFLUX_ORG, INFLUX_BUCKET)
	go func() {
		for err := range writeAPI.Errors() {
			log.Printf("❌ InfluxDB Write Error: %s\n", err.Error())
		}
	}()

	// MQTT setup
	dataChannel := make(chan SensorData, 1000)
	go influxWorker(writeAPI, dataChannel)

	var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		var payload SensorPayload
		if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
			log.Printf("⚠️ Failed to parse JSON: %s\n", string(msg.Payload()))
			return
		}
		topicParts := strings.Split(msg.Topic(), "/")
		deviceID := topicParts[len(topicParts)-1]
		dataChannel <- SensorData{DeviceID: deviceID, Payload: payload, Timestamp: time.Now()}
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(MQTT_BROKER)
	opts.SetClientID(MQTT_CLIENT_ID)
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("✅ Connected to MQTT Broker!")
		if token := client.Subscribe(MQTT_TOPIC, 0, nil); token.Wait() && token.Error() != nil {
			log.Fatalf("❌ Subscription error: %s\n", token.Error())
		}
		log.Printf("📡 Subscribed to: %s\n", MQTT_TOPIC)
	})

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ MQTT connection error: %s\n", token.Error())
	}

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleDashboard)
	mux.HandleFunc("/api/data", handleAPIData)
	mux.Handle("/static/", http.FileServer(http.FS(static)))

	server := &http.Server{Addr: HTTP_PORT, Handler: mux}
	go func() {
		log.Printf("🌐 Dashboard at http://localhost%s\n", HTTP_PORT)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %s", err)
		}
	}()

	// Telegram bot
	if TELEGRAM_BOT_TOKEN != "" {
		go startTelegramBot(mqttClient)
	} else {
		log.Println("⚠️ Telegram bot disabled (no token)")
	}

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("\nShutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	mqttClient.Disconnect(250)
	close(dataChannel)
	writeAPI.Flush()
	log.Println("👋 Done.")
}

func influxWorker(writeAPI api.WriteAPI, dataCh chan SensorData) {
	for data := range dataCh {
		tags := map[string]string{
			"device_id": data.DeviceID,
			"unit":      data.Payload.Unit,
		}
		fields := map[string]interface{}{
			"suhu":   data.Payload.Suhu,
			"status": data.Payload.Status,
		}
		writeAPI.WritePoint(influxdb2.NewPoint("sensor_logs", tags, fields, data.Timestamp))
		log.Printf("💾 Device: %s | Suhu: %.2f%s | Status: %s\n",
			data.DeviceID, data.Payload.Suhu, data.Payload.Unit, data.Payload.Status)
	}
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	tmpl, err := templates.ReadFile("templates/dashboard.html")
	if err != nil {
		http.Error(w, "template error", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(tmpl)
}

func handleAPIData(w http.ResponseWriter, r *http.Request) {
	rangeParam := r.URL.Query().Get("range")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")

	// Custom date range takes priority
	if start != "" && end != "" {
		data := queryInfluxDataRange(start, end)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
		return
	}

	if rangeParam != "1h" && rangeParam != "24h" && rangeParam != "7d" {
		rangeParam = "24h"
	}

	data := queryInfluxData(rangeParam)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func queryInfluxData(timeRange string) DashboardData {
	queryAPI := influxClient.QueryAPI(INFLUX_ORG)
	result := DashboardData{Range: timeRange, History: []HistoryPoint{}, Logs: []LogEntry{}}

	// Query: latest reading
	fluxCurrent := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: -1h)
  |> filter(fn: (r) => r._field == "suhu" or r._field == "status")
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`, INFLUX_BUCKET)

	if tables, err := queryAPI.Query(context.Background(), fluxCurrent); err != nil {
		log.Printf("❌ Current query error: %s", err)
	} else {
		for tables.Next() {
			rec := tables.Record()
			suhu, _ := rec.ValueByKey("suhu").(float64)
			deviceID, _ := rec.ValueByKey("device_id").(string)
			unit, _ := rec.ValueByKey("unit").(string)
			status, _ := rec.ValueByKey("status").(string)
			if unit == "" {
				unit = "C"
			}
			if status == "" {
				status = "AMAN"
			}
			result.Current = &CurrentState{
				DeviceID: deviceID,
				Suhu:     suhu,
				Unit:     unit,
				Status:   status,
				Time:     rec.Time().Format(time.RFC3339),
			}
			break
		}
	}

	// Query: history aggregated per window based on range
	window := "5m"
	if timeRange == "7d" {
		window = "1h"
	}
	fluxHistory := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: -%s)
  |> filter(fn: (r) => r._field == "suhu")
  |> aggregateWindow(every: %s, fn: mean, createEmpty: true)
  |> fill(value: 0.0)
  |> keep(columns: ["_time", "_value"])`, INFLUX_BUCKET, timeRange, window)

	if tables, err := queryAPI.Query(context.Background(), fluxHistory); err != nil {
		log.Printf("❌ History query error: %s", err)
	} else {
		for tables.Next() {
			rec := tables.Record()
			val, ok := rec.Value().(float64)
			hp := HistoryPoint{Time: rec.Time().Format(time.RFC3339)}
			if ok {
				hp.Value = &val
			}
			result.History = append(result.History, hp)
		}
	}

	// Query: last 50 log entries
	fluxLogs := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: -24h)
  |> filter(fn: (r) => r._field == "suhu" or r._field == "status")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 50)`, INFLUX_BUCKET)

	if tables, err := queryAPI.Query(context.Background(), fluxLogs); err != nil {
		log.Printf("❌ Logs query error: %s", err)
	} else {
		for tables.Next() {
			rec := tables.Record()
			suhu, ok := rec.ValueByKey("suhu").(float64)
			deviceID, _ := rec.ValueByKey("device_id").(string)
			status, _ := rec.ValueByKey("status").(string)
			unit, _ := rec.ValueByKey("unit").(string)
			if unit == "" {
				unit = "C"
			}
			if status == "" {
				status = "—"
			}
			if deviceID == "" {
				deviceID = "—"
			}
			entry := LogEntry{
				Time:     rec.Time().Format(time.RFC3339),
				DeviceID: deviceID,
				Status:   status,
				Unit:     unit,
			}
			if ok {
				entry.Suhu = &suhu
			}
			result.Logs = append(result.Logs, entry)
		}
	}

	return result
}

func queryInfluxDataRange(startStr, endStr string) DashboardData {
	startTime, err1 := time.Parse("2006-01-02", startStr)
	endTime, err2 := time.Parse("2006-01-02", endStr)
	if err1 != nil || err2 != nil {
		return queryInfluxData("24h")
	}
	// End date should include the full day
	endTime = endTime.Add(24*time.Hour - time.Second)

	days := endTime.Sub(startTime).Hours() / 24
	window := "5m"
	if days > 7 {
		window = "3h"
	} else if days > 1 {
		window = "1h"
	}

	queryAPI := influxClient.QueryAPI(INFLUX_ORG)
	result := DashboardData{Range: "custom", History: []HistoryPoint{}, Logs: []LogEntry{}}

	start := startTime.Format(time.RFC3339)
	end := endTime.Format(time.RFC3339)

	// Current: still last 1h
	fluxCurrent := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: -1h)
  |> filter(fn: (r) => r._field == "suhu" or r._field == "status")
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`, INFLUX_BUCKET)

	if tables, err := queryAPI.Query(context.Background(), fluxCurrent); err == nil {
		for tables.Next() {
			rec := tables.Record()
			suhu, _ := rec.ValueByKey("suhu").(float64)
			deviceID, _ := rec.ValueByKey("device_id").(string)
			unit, _ := rec.ValueByKey("unit").(string)
			status, _ := rec.ValueByKey("status").(string)
			if unit == "" { unit = "C" }
			if status == "" { status = "AMAN" }
			result.Current = &CurrentState{
				DeviceID: deviceID, Suhu: suhu, Unit: unit, Status: status,
				Time: rec.Time().Format(time.RFC3339),
			}
			break
		}
	}

	// History with custom range
	fluxHistory := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._field == "suhu")
  |> aggregateWindow(every: %s, fn: mean, createEmpty: true)
  |> fill(value: 0.0)
  |> keep(columns: ["_time", "_value"])`, INFLUX_BUCKET, start, end, window)

	if tables, err := queryAPI.Query(context.Background(), fluxHistory); err == nil {
		for tables.Next() {
			rec := tables.Record()
			val, ok := rec.Value().(float64)
			hp := HistoryPoint{Time: rec.Time().Format(time.RFC3339)}
			if ok { hp.Value = &val }
			result.History = append(result.History, hp)
		}
	}

	// Logs within custom range
	fluxLogs := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._field == "suhu" or r._field == "status")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 50)`, INFLUX_BUCKET, start, end)

	if tables, err := queryAPI.Query(context.Background(), fluxLogs); err == nil {
		for tables.Next() {
			rec := tables.Record()
			suhu, ok := rec.ValueByKey("suhu").(float64)
			deviceID, _ := rec.ValueByKey("device_id").(string)
			status, _ := rec.ValueByKey("status").(string)
			unit, _ := rec.ValueByKey("unit").(string)
			if unit == "" { unit = "C" }
			if status == "" { status = "—" }
			if deviceID == "" { deviceID = "—" }
			entry := LogEntry{Time: rec.Time().Format(time.RFC3339), DeviceID: deviceID, Status: status, Unit: unit}
			if ok { entry.Suhu = &suhu }
			result.Logs = append(result.Logs, entry)
		}
	}

	return result
}

func startTelegramBot(mqttClient mqtt.Client) {
	bot, err := tgbotapi.NewBotAPI(TELEGRAM_BOT_TOKEN)
	if err != nil {
		log.Printf("❌ Telegram bot error: %s", err)
		return
	}
	log.Printf("🤖 Telegram bot @%s started", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}

		chatID := update.Message.Chat.ID
		text := strings.TrimSpace(update.Message.Text)

		switch {
		case text == "/start" || text == "/help":
			reply := `🌡 *Brake Temperature Bot*

Commands:
/publish — Publish random sensor reading
/publish <suhu> — Publish specific temperature (e.g. /publish 72.5)
/status — Check service status & latest reading`
			msg := tgbotapi.NewMessage(chatID, reply)
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		case text == "/status":
			data := queryInfluxData("1h")
			var reply string
			if data.Current != nil {
				reply = fmt.Sprintf("✅ *Service Online*\n\n"+
					"📡 MQTT: Connected\n"+
					"🗄 InfluxDB: Connected\n"+
					"🌐 Dashboard: http://localhost%s\n\n"+
					"*Latest Reading:*\n"+
					"🌡 Temperature: %.2f°%s\n"+
					"📟 Device: %s\n"+
					"⚠️ Status: %s\n"+
					"🕐 Time: %s",
					HTTP_PORT, data.Current.Suhu, data.Current.Unit,
					data.Current.DeviceID, data.Current.Status, data.Current.Time)
			} else {
				reply = "✅ *Service Online*\n\n📡 MQTT: Connected\n🗄 InfluxDB: Connected\n\n⚠️ No sensor data in the last hour"
			}
			msg := tgbotapi.NewMessage(chatID, reply)
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		case text == "/publish" || strings.HasPrefix(text, "/publish "):
			var suhu float64
			parts := strings.Fields(text)
			if len(parts) > 1 {
				val, err := strconv.ParseFloat(parts[1], 64)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid value. Usage: /publish 72.5"))
					continue
				}
				suhu = val
			} else {
				suhu = 30 + rand.Float64()*50
			}

			status := "AMAN"
			if suhu > 60 {
				status = "PERINGATAN"
			}
			if suhu > 80 {
				status = "BAHAYA"
			}

			payload := fmt.Sprintf(`{"suhu": %.2f, "status": "%s", "unit": "C"}`, suhu, status)
			token := mqttClient.Publish(MQTT_TOPIC, 1, false, payload)
			token.Wait()

			reply := fmt.Sprintf("📤 *Published!*\n\n🌡 Suhu: %.2f°C\n⚠️ Status: %s", suhu, status)
			msg := tgbotapi.NewMessage(chatID, reply)
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		default:
			bot.Send(tgbotapi.NewMessage(chatID, "Unknown command. Use /help"))
		}
	}
}

func loadEnv() {
	env := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	MQTT_BROKER = env("MQTT_BROKER", "tcp://localhost:1883")
	MQTT_TOPIC = env("MQTT_TOPIC", "brake/temperature")
	MQTT_CLIENT_ID = env("MQTT_CLIENT_ID", "go_mqtt_forwarder")
	INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
	INFLUX_TOKEN = env("INFLUX_TOKEN", "")
	INFLUX_ORG = env("INFLUX_ORG", "")
	INFLUX_BUCKET = env("INFLUX_BUCKET", "brake-sensor")
	HTTP_PORT = env("HTTP_PORT", ":8080")
	TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", "")
}
