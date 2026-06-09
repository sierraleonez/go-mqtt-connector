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
	MQTT_BROKER        string
	MQTT_GPS_TOPIC     string
	MQTT_CLIENT_ID     string
	INFLUX_URL         string
	INFLUX_TOKEN       string
	INFLUX_ORG         string
	INFLUX_BUCKET      string // now points to brake-gps bucket
	HTTP_PORT          string
	TELEGRAM_BOT_TOKEN string
	TELEGRAM_ENABLED   bool
)

type PositionLogEntry struct {
	Latitude   float64 `json:"lat"`
	Longitude  float64 `json:"lng"`
	Speed      float64 `json:"speed"`
	Altitude   float64 `json:"altitude"`
	Time       string  `json:"time"`
	Satellites int     `json:"satellites"`
	Status     string  `json:"status"`
	Suhu       float64 `json:"suhu,omitempty"`
}

var influxClient influxdb2.Client

func main() {
	godotenv.Load()
	loadEnv()

	log.Println("🚀 Starting MQTT Forwarder + Dashboard Server...")

	influxClient = influxdb2.NewClient(INFLUX_URL, INFLUX_TOKEN)
	defer influxClient.Close()

	gpsWriteAPI := influxClient.WriteAPI(INFLUX_ORG, INFLUX_BUCKET)
	go func() {
		for err := range gpsWriteAPI.Errors() {
			log.Printf("❌ InfluxDB GPS Write Error: %s\n", err.Error())
		}
	}()

	// MQTT setup
	gpsChannel := make(chan PositionLogEntry, 1000)
	go gpsInfluxWorker(gpsWriteAPI, gpsChannel)

	var gpsMessageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		var entry PositionLogEntry
		if err := json.Unmarshal(msg.Payload(), &entry); err != nil {
			log.Printf("⚠️ Failed to parse GPS JSON: %s\n", string(msg.Payload()))
			return
		}
		gpsChannel <- entry
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(MQTT_BROKER)
	opts.SetClientID(MQTT_CLIENT_ID + fmt.Sprintf("_%d", time.Now().UnixNano()%10000))
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("✅ Connected to MQTT Broker!")
		if token := client.Subscribe(MQTT_GPS_TOPIC, 0, gpsMessageHandler); token.Wait() && token.Error() != nil {
			log.Fatalf("❌ GPS Subscription error: %s\n", token.Error())
		}
		log.Printf("📡 Subscribed to: %s\n", MQTT_GPS_TOPIC)
	})

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ MQTT connection error: %s\n", token.Error())
	}

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleDashboard)
	mux.HandleFunc("/api/gps", handleAPIGps)
	mux.HandleFunc("/api/gps/export", handleAPIGpsExport)
	mux.HandleFunc("/api/gps/history", handleAPIGpsHistory)
	mux.Handle("/static/", http.FileServer(http.FS(static)))

	server := &http.Server{Addr: HTTP_PORT, Handler: mux}
	go func() {
		log.Printf("🌐 Dashboard at http://localhost%s\n", HTTP_PORT)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %s", err)
		}
	}()

	// Telegram bot
	if TELEGRAM_ENABLED && TELEGRAM_BOT_TOKEN != "" {
		go startTelegramBot(mqttClient)
	} else {
		log.Println("⚠️ Telegram bot disabled")
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
	close(gpsChannel)
	gpsWriteAPI.Flush()
	log.Println("👋 Done.")
}

func gpsInfluxWorker(writeAPI api.WriteAPI, dataCh chan PositionLogEntry) {
	for entry := range dataCh {
		ts := time.Now()
		if entry.Time != "" {
			if parsed, err := time.Parse(time.RFC3339, entry.Time); err == nil {
				ts = parsed
			}
		}
		fields := map[string]interface{}{
			"latitude":   entry.Latitude,
			"longitude":  entry.Longitude,
			"speed":      entry.Speed,
			"altitude":   entry.Altitude,
			"satellites": entry.Satellites,
			"status":     entry.Status,
			"suhu":       entry.Suhu,
		}
		writeAPI.WritePoint(influxdb2.NewPoint("gps_logs", nil, fields, ts))
		log.Printf("📍 GPS | Lat: %.6f | Lng: %.6f | Speed: %.1f\n",
			entry.Latitude, entry.Longitude, entry.Speed)
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

func handleAPIGps(w http.ResponseWriter, r *http.Request) {
	queryGps(w, r, 100)
}

func handleAPIGpsExport(w http.ResponseWriter, r *http.Request) {
	queryGps(w, r, 0) // 0 = no limit
}

func queryGps(w http.ResponseWriter, r *http.Request, limit int) {
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	rangeParam := r.URL.Query().Get("range")

	var rangeClause string
	if start != "" && end != "" {
		startTime, err1 := time.Parse("2006-01-02", start)
		endTime, err2 := time.Parse("2006-01-02", end)
		if err1 != nil || err2 != nil {
			http.Error(w, "invalid date format, use YYYY-MM-DD", http.StatusBadRequest)
			return
		}
		endTime = endTime.Add(24*time.Hour - time.Second)
		rangeClause = fmt.Sprintf(`range(start: %s, stop: %s)`, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	} else {
		switch rangeParam {
		case "1h":
			rangeClause = `range(start: -1h)`
		case "7d":
			rangeClause = `range(start: -7d)`
		default:
			rangeClause = `range(start: -24h)`
		}
	}

	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf("\n  |> limit(n: %d)", limit)
	}

	queryAPI := influxClient.QueryAPI(INFLUX_ORG)
	flux := fmt.Sprintf(`from(bucket: "%s")
  |> %s
  |> filter(fn: (r) => r._field == "latitude" or r._field == "longitude" or r._field == "speed" or r._field == "altitude" or r._field == "satellites" or r._field == "status" or r._field == "suhu")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r.latitude != 0.0 and r.longitude != 0.0)
  |> sort(columns: ["_time"], desc: true)%s`, INFLUX_BUCKET, rangeClause, limitClause)

	var points []map[string]interface{}
	if tables, err := queryAPI.Query(context.Background(), flux); err == nil {
		for tables.Next() {
			rec := tables.Record()
			lat, _ := rec.ValueByKey("latitude").(float64)
			lng, _ := rec.ValueByKey("longitude").(float64)
			speed, _ := rec.ValueByKey("speed").(float64)
			alt, _ := rec.ValueByKey("altitude").(float64)
			sat, _ := rec.ValueByKey("satellites").(int64)
			status, _ := rec.ValueByKey("status").(string)
			suhu, _ := rec.ValueByKey("suhu").(float64)
			points = append(points, map[string]interface{}{
				"lat": lat, "lng": lng, "speed": speed,
				"altitude": alt, "satellites": sat, "status": status,
				"suhu": suhu,
				"time": rec.Time().Format(time.RFC3339),
			})
		}
	}
	if points == nil {
		points = []map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(points)
}

func handleAPIGpsHistory(w http.ResponseWriter, r *http.Request) {
	rangeParam := r.URL.Query().Get("range")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")

	var rangeClause, window string
	if start != "" && end != "" {
		startTime, err1 := time.Parse("2006-01-02", start)
		endTime, err2 := time.Parse("2006-01-02", end)
		if err1 != nil || err2 != nil {
			http.Error(w, "invalid date format, use YYYY-MM-DD", http.StatusBadRequest)
			return
		}
		endTime = endTime.Add(24*time.Hour - time.Second)
		days := endTime.Sub(startTime).Hours() / 24
		if days > 7 {
			window = "3h"
		} else if days > 1 {
			window = "1h"
		} else {
			window = "5m"
		}
		rangeClause = fmt.Sprintf(`range(start: %s, stop: %s)`, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	} else {
		switch rangeParam {
		case "1h":
			rangeClause, window = `range(start: -1h)`, "1m"
		case "7d":
			rangeClause, window = `range(start: -7d)`, "1h"
		default:
			rangeClause, window = `range(start: -24h)`, "5m"
		}
	}

	queryAPI := influxClient.QueryAPI(INFLUX_ORG)
	flux := fmt.Sprintf(`from(bucket: "%s")
  |> %s
  |> filter(fn: (r) => r._field == "suhu")
  |> aggregateWindow(every: %s, fn: mean, createEmpty: true)
  |> fill(value: 0.0)
  |> keep(columns: ["_time", "_value"])`, INFLUX_BUCKET, rangeClause, window)

	type point struct {
		Time  string   `json:"time"`
		Value *float64 `json:"value"`
	}
	var history []point
	if tables, err := queryAPI.Query(context.Background(), flux); err == nil {
		for tables.Next() {
			rec := tables.Record()
			p := point{Time: rec.Time().Format(time.RFC3339)}
			if v, ok := rec.Value().(float64); ok {
				p.Value = &v
			}
			history = append(history, p)
		}
	}
	if history == nil {
		history = []point{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
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
			reply := `🛰 *Brake GPS Bot*

Commands:
/gps — Publish random GPS location
/status — Check service status`
			msg := tgbotapi.NewMessage(chatID, reply)
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		case text == "/status":
			reply := fmt.Sprintf("✅ *Service Online*\n\n📡 MQTT: Connected\n🗄 InfluxDB: Connected\n🌐 Dashboard: http://localhost%s", HTTP_PORT)
			msg := tgbotapi.NewMessage(chatID, reply)
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		case text == "/publish" || strings.HasPrefix(text, "/publish "):
			bot.Send(tgbotapi.NewMessage(chatID, "❌ /publish is no longer supported. Use /gps to publish GPS data."))

		case text == "/gps":
			lat := -6.1 - rand.Float64()*0.2
			lng := 106.7 + rand.Float64()*0.3
			speed := rand.Float64() * 80
			alt := 10 + rand.Float64()*50
			sat := 4 + rand.Intn(9)
			statuses := []string{"AMAN", "PERINGATAN", "BAHAYA"}
			status := statuses[rand.Intn(3)]
			payload := fmt.Sprintf(`{"lat":%.6f,"lng":%.6f,"speed":%.1f,"altitude":%.1f,"satellites":%d,"status":"%s","time":"%s"}`,
				lat, lng, speed, alt, sat, status, time.Now().Format(time.RFC3339))
			token := mqttClient.Publish(MQTT_GPS_TOPIC, 1, false, payload)
			token.Wait()
			reply := fmt.Sprintf("📍 *GPS Published!*\n\nLat: %.6f\nLng: %.6f\nSpeed: %.1f km/h\nStatus: %s", lat, lng, speed, status)
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
	MQTT_GPS_TOPIC = env("MQTT_GPS_TOPIC", "brake/gps")
	MQTT_CLIENT_ID = env("MQTT_CLIENT_ID", "go_mqtt_forwarder")
	INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
	INFLUX_TOKEN = env("INFLUX_TOKEN", "")
	INFLUX_ORG = env("INFLUX_ORG", "")
	INFLUX_BUCKET = env("INFLUX_BUCKET", "brake-gps")
	HTTP_PORT = ":" + env("HTTP_PORT", "8080")
	TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", "")
	TELEGRAM_ENABLED = env("TELEGRAM_ENABLED", "false") == "true"
}
