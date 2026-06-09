# Role & Goal
You are an expert Go developer and frontend engineer. Please update our existing Go MQTT-to-InfluxDB forwarder and its accompanying HTML dashboard according to the requirements below.

---

## Backend Updates (Go)

### 1. InfluxDB Bucket Migration
* Change the InfluxDB bucket configuration so that **all** handlers now write to the `brake-gps` bucket. 
* Remove or deprecate any references to old bucket names if they are no longer needed.

### 2. MQTT Topic Subscription Change
* Update the MQTT client initialization and subscription logic.
* **Stop** listening to the `brake/temperature` topic.
* **Only** subscribe and listen to the `brake/gps` topic.

### 3. Brake-GPS Handler Filter Update
* Update the `brake-gps` handler to include a date-range filter.
* This filter should mimic the exact date-range querying logic currently found in the `queryInfluxData` function (e.g., parsing `start` and `end` timestamps from the request/payload to filter the InfluxDB query).

---

## Frontend Updates (HTML/JavaScript Dashboard)

### 4. Data Table Expansion
* Update the data table in the HTML dashboard to display the following new telemetry fields:
  * **Altitude**
  * **Longitude**
  * **Speed**
  * **Satellite Count**
* Ensure the table headers and data rows map correctly to the incoming JSON structure from the `brake/gps` topic.

### 5. Live Speed Gauge (WebSocket)
* Add a visual Gauge component to the dashboard to display **Speed** in real-time.
* This gauge must consume live data streamed via WebSockets originating from the `brake/gps` MQTT topic.
* *Note:* You can use a lightweight library like Canvas-Gauges, JustGage, or basic Tailwind/CSS transitions for the gauge—keep it clean and modern.

---

## Output Requirements
1. Provide the updated Go code blocks clearly, highlighting where the changes were made.
2. Provide the updated HTML/JavaScript code for the dashboard.
3. Ensure proper error handling for the new date-range filtering and missing JSON fields.