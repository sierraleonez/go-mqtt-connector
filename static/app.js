let currentStatusFilter = '';
let allLogs = [];
let gpsMap = null;
let gpsTrail = null;
let gpsMarkers = [];
let chart = null;
let currentRange = '24h';
let customStart = '';
let customEnd = '';

const MAX_SPEED = 120; // km/h for gauge full-scale
const GAUGE_ARC = 283; // circumference of the arc path (π * r ≈ 283 for r=90)

// --- Speed Gauge ---
function updateSpeedGauge(speed) {
    const fill = document.getElementById('speed-fill');
    const valueEl = document.getElementById('speed-value');
    if (!fill || !valueEl) return;

    const pct = Math.min(Math.max(speed / MAX_SPEED, 0), 1);
    const color = pct < 0.5 ? '#3b82f6' : pct < 0.8 ? '#eab308' : '#ef4444';
    fill.setAttribute('stroke', color);
    fill.setAttribute('stroke-dasharray', `${pct * GAUGE_ARC} ${GAUGE_ARC}`);
    valueEl.textContent = speed.toFixed(1);
}

// --- Suhu Gauge ---
function updateSuhuGauge(suhu) {
    const fill = document.getElementById('suhu-fill');
    const valueEl = document.getElementById('suhu-value');
    if (!fill || !valueEl) return;

    const pct = Math.min(Math.max(suhu / 100, 0), 1);
    const color = pct < 0.5 ? '#22c55e' : pct < 0.75 ? '#eab308' : '#ef4444';
    fill.setAttribute('stroke', color);
    fill.setAttribute('stroke-dasharray', `${pct * GAUGE_ARC} ${GAUGE_ARC}`);
    valueEl.textContent = suhu.toFixed(1);
}

// --- Telemetry cards ---
function updateCards(p) {
    document.getElementById('card-lat').textContent = p.lat != null ? p.lat.toFixed(6) : '—';
    document.getElementById('card-lng').textContent = p.lng != null ? p.lng.toFixed(6) : '—';
    document.getElementById('card-alt').textContent = p.altitude != null ? p.altitude.toFixed(1) + ' m' : '—';
    document.getElementById('card-sat').textContent = p.satellites != null ? p.satellites : '—';

    if (p.suhu != null) updateSuhuGauge(p.suhu);

    const badge = document.getElementById('status-badge');
    const styles = { AMAN: 'bg-green-500 text-white', PERINGATAN: 'bg-yellow-500 text-black', BAHAYA: 'bg-red-500 text-white' };
    const status = p.status || 'AMAN';
    badge.className = `rounded-full px-4 py-1 text-sm font-bold tracking-wide ${styles[status] || styles.AMAN}`;
    badge.textContent = status;

    const lastUpdated = document.getElementById('last-updated');
    const ts = p.time ? new Date(p.time) : new Date();
    const ago = Math.floor((Date.now() - ts.getTime()) / 1000);
    lastUpdated.textContent = `Last updated: ${ago < 60 ? ago + 's ago' : Math.floor(ago / 60) + 'm ago'}`;
}

// --- Logs table ---
function updateLogs(logs) {
    const tbody = document.getElementById('logs-body');
    if (!logs || logs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="px-4 py-6 text-center text-slate-500">No GPS data available.</td></tr>';
        return;
    }
    tbody.innerHTML = logs.map(row => {
        const statusClass = row.status === 'BAHAYA' ? 'bg-red-500/20 text-red-400' :
                            row.status === 'PERINGATAN' ? 'bg-yellow-500/20 text-yellow-400' :
                            'bg-green-500/20 text-green-400';
        return `<tr class="border-b border-slate-800 hover:bg-slate-800/50">
            <td class="px-4 py-2 text-slate-400 whitespace-nowrap">${new Date(row.time).toLocaleString()}</td>
            <td class="px-4 py-2 font-mono text-slate-300">${row.lat != null ? row.lat.toFixed(6) : '—'}</td>
            <td class="px-4 py-2 font-mono text-slate-300">${row.lng != null ? row.lng.toFixed(6) : '—'}</td>
            <td class="px-4 py-2 text-slate-300">${row.altitude != null ? row.altitude.toFixed(1) : '—'}</td>
            <td class="px-4 py-2 text-slate-300">${row.speed != null ? row.speed.toFixed(1) : '—'}</td>
            <td class="px-4 py-2 text-slate-300">${row.suhu != null && row.suhu !== 0 ? row.suhu.toFixed(2) : '—'}</td>
            <td class="px-4 py-2 text-slate-300">${row.satellites != null ? row.satellites : '—'}</td>
            <td class="px-4 py-2"><span class="rounded-full px-2 py-0.5 text-xs font-semibold ${statusClass}">${row.status || '—'}</span></td>
        </tr>`;
    }).join('');
}

function filteredLogs() {
    if (!currentStatusFilter) return allLogs;
    return allLogs.filter(l => l.status === currentStatusFilter);
}

function filterStatus(status) {
    currentStatusFilter = status;
    document.querySelectorAll('.status-btn').forEach(btn => {
        const active = btn.dataset.status === status;
        if (active) {
            btn.className = 'status-btn rounded-md px-3 py-1 text-xs font-medium bg-slate-700 text-white';
        } else {
            const colors = { '': 'text-slate-300', 'AMAN': 'text-green-400', 'PERINGATAN': 'text-yellow-400', 'BAHAYA': 'text-red-400' };
            btn.className = `status-btn rounded-md px-3 py-1 text-xs font-medium ${colors[btn.dataset.status] || 'text-slate-300'} border border-slate-600`;
        }
    });
    updateLogs(filteredLogs());
}

// --- GPS API fetch ---
async function fetchGpsLogs(start, end) {
    try {
        let url = `/api/gps?range=${currentRange}`;
        if (start && end) url = `/api/gps?start=${start}&end=${end}`;
        else if (customStart && customEnd) url = `/api/gps?start=${customStart}&end=${customEnd}`;
        const res = await fetch(url);
        const points = await res.json();
        allLogs = points || [];
        updateLogs(filteredLogs());
        if (gpsMap && allLogs.length) {
            allLogs.forEach(p => addGpsPoint(p, false));
            if (gpsTrail.getBounds().isValid()) gpsMap.fitBounds(gpsTrail.getBounds(), { padding: [30, 30] });
        }
    } catch (e) { console.error('GPS fetch error:', e); }
}

// --- Chart ---
async function fetchHistory() {
    try {
        let url = `/api/gps/history?range=${currentRange}`;
        if (customStart && customEnd) url = `/api/gps/history?start=${customStart}&end=${customEnd}`;
        const res = await fetch(url);
        const data = await res.json();
        updateChart(data || []);
    } catch (e) { console.error('History fetch error:', e); }
}

function updateChart(history) {
    const ctx = document.getElementById('history-chart');
    if (!ctx) return;
    const data = history.map(h => ({ x: new Date(h.time), y: h.value }));

    let timeUnit = 'hour';
    if (currentRange === '1h') timeUnit = 'minute';
    else if (currentRange === '7d') timeUnit = 'day';
    else if (customStart && customEnd) {
        const days = (new Date(customEnd) - new Date(customStart)) / 86400000;
        timeUnit = days > 7 ? 'day' : days > 1 ? 'hour' : 'minute';
    }

    if (chart) {
        chart.data.datasets[0].data = data;
        chart.options.scales.x.time.unit = timeUnit;
        chart.update('none');
        return;
    }
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Suhu (°C)',
                data,
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59,130,246,0.1)',
                fill: true,
                tension: 0.3,
                pointRadius: data.length < 20 ? 3 : 0,
                borderWidth: 2,
                spanGaps: true,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { type: 'time', time: { unit: timeUnit }, grid: { color: '#1e293b' }, ticks: { color: '#64748b', font: { size: 11 } } },
                y: { grid: { color: '#1e293b' }, ticks: { color: '#64748b', font: { size: 11 }, callback: v => v + '°' } }
            },
            interaction: { intersect: false, mode: 'index' },
        }
    });
}

function changeRange(r) {
    currentRange = r;
    customStart = '';
    customEnd = '';
    document.getElementById('date-start').value = '';
    document.getElementById('date-end').value = '';
    document.querySelectorAll('.range-btn').forEach(btn => {
        const active = btn.dataset.range === r;
        btn.className = `range-btn rounded-md px-4 py-1.5 text-sm font-medium ${active ? 'bg-blue-600 text-white' : 'border border-slate-600 text-slate-400 hover:border-slate-400 hover:text-slate-200'}`;
    });
    if (chart) { chart.destroy(); chart = null; }
    fetchHistory();
    fetchGpsLogs();
}

function applyDateRange() {
    const start = document.getElementById('date-start').value;
    const end = document.getElementById('date-end').value;
    if (!start || !end) return;
    if (start > end) { alert('Start date must be before end date'); return; }
    customStart = start;
    customEnd = end;
    currentRange = 'custom';
    document.querySelectorAll('.range-btn').forEach(btn => {
        btn.className = 'range-btn rounded-md px-4 py-1.5 text-sm font-medium border border-slate-600 text-slate-400 hover:border-slate-400 hover:text-slate-200';
    });
    if (chart) { chart.destroy(); chart = null; }
    fetchHistory();
    fetchGpsLogs();
}

// --- Tab switching ---
function switchTab(tab) {
    document.getElementById('panel-live').classList.toggle('hidden', tab !== 'live');
    document.getElementById('panel-chart').classList.toggle('hidden', tab !== 'chart');
    document.getElementById('panel-map').classList.toggle('hidden', tab !== 'map');
    ['live', 'chart', 'map'].forEach(t => {
        document.getElementById('tab-' + t).className = `rounded-lg px-5 py-2 text-sm font-medium ${tab === t ? 'bg-slate-700 text-white' : 'text-slate-400 hover:text-slate-200'}`;
    });
    if (tab === 'chart') fetchHistory();
    if (tab === 'map') {
        if (!gpsMap) initMap();
        else gpsMap.invalidateSize();
    }
}

// --- Map date range ---
function applyMapDateRange() {
    const start = document.getElementById('map-date-start').value;
    const end = document.getElementById('map-date-end').value;
    if (!start || !end) return;
    if (start > end) { alert('Start date must be before end date'); return; }
    // Clear existing markers
    gpsMarkers.forEach(m => m.remove());
    gpsMarkers = [];
    gpsTrail.setLatLngs([]);
    fetchGpsLogs(start, end);
}

// --- Map ---
const statusColor = { AMAN: '#22c55e', PERINGATAN: '#eab308', BAHAYA: '#ef4444' };

function initMap() {
    gpsMap = L.map('map').setView([-6.2, 106.8], 13);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
    }).addTo(gpsMap);
    gpsTrail = L.polyline([], { color: '#3b82f6', weight: 3 }).addTo(gpsMap);
    fetchGpsLogs();
}

function addGpsPoint(p, pan) {
    if (p.lat == null || p.lng == null) return;
    const ll = [p.lat, p.lng];
    const color = statusColor[p.status] || statusColor.AMAN;
    const marker = L.circleMarker(ll, { radius: 6, color, fillColor: color, fillOpacity: 0.8, weight: 1 })
        .bindPopup(`<b>${p.status || 'AMAN'}</b><br>Speed: ${p.speed} km/h<br>Alt: ${p.altitude} m<br>Sat: ${p.satellites}<br>${new Date(p.time).toLocaleString()}`)
        .addTo(gpsMap);
    gpsMarkers.push(marker);
    gpsTrail.addLatLng(ll);
    if (pan) gpsMap.panTo(ll);
}

// --- Init ---
fetchGpsLogs();
setInterval(fetchGpsLogs, 30000);

// --- MQTT WebSocket: brake/gps only ---
const mqttClient = mqtt.connect('ws://43.159.61.247:9001');
mqttClient.on('connect', () => {
    mqttClient.subscribe('brake/gps');
    console.log('🟢 MQTT connected, subscribed to brake/gps');
});
mqttClient.on('message', (topic, message) => {
    try {
        const p = JSON.parse(message.toString());
        if (topic !== 'brake/gps') return;

        // Update speed gauge
        updateSpeedGauge(p.speed || 0);
        // Update telemetry cards
        updateCards(p);
        // Prepend to live logs
        allLogs.unshift({ ...p, time: p.time || new Date().toISOString() });
        if (allLogs.length > 100) allLogs.pop();
        updateLogs(filteredLogs());
        // Update map if active
        if (gpsMap) addGpsPoint(p, true);
    } catch (e) { console.error('MQTT parse error:', e); }
});
