let currentRange = '24h';
let customStart = '';
let customEnd = '';
let chart = null;
let currentStatusFilter = '';
let allLogs = [];

// --- Gauge ---
function updateGauge(current) {
    const arc = Math.PI * 90;
    const gaugeFill = document.getElementById('gauge-fill');
    const gaugeValue = document.getElementById('gauge-value');
    const gaugeUnit = document.getElementById('gauge-unit');
    const lastUpdated = document.getElementById('last-updated');
    const deviceId = document.getElementById('device-id');
    const badge = document.getElementById('status-badge');

    if (!current) {
        gaugeValue.textContent = '—';
        lastUpdated.textContent = 'Waiting for sensor data…';
        return;
    }

    const pct = Math.min(Math.max(current.suhu / 100, 0), 1);
    const color = pct < 0.5 ? '#22c55e' : pct < 0.75 ? '#eab308' : '#ef4444';

    gaugeFill.setAttribute('stroke', color);
    gaugeFill.setAttribute('stroke-dasharray', `${pct * arc} ${arc}`);
    gaugeValue.textContent = current.suhu.toFixed(2);
    gaugeUnit.textContent = `°${current.unit || 'C'}`;
    deviceId.textContent = current.device_id || '—';

    const styles = { AMAN: 'bg-green-500 text-white', PERINGATAN: 'bg-yellow-500 text-black', BAHAYA: 'bg-red-500 text-white' };
    badge.className = `rounded-full px-4 py-1 text-sm font-bold tracking-wide ${styles[current.status] || styles.AMAN}`;
    badge.textContent = current.status || 'AMAN';

    const ago = Math.floor((Date.now() - new Date(current.time).getTime()) / 1000);
    lastUpdated.textContent = `Last updated: ${ago < 60 ? ago + 's ago' : Math.floor(ago / 60) + 'm ago'}`;
}

// --- Chart ---
function updateChart(history) {
    const ctx = document.getElementById('history-chart');
    const data = history.map(h => ({ x: new Date(h.time), y: h.value }));

    let timeUnit = 'hour';
    if (currentRange === '1h') timeUnit = 'minute';
    else if (currentRange === '7d') timeUnit = 'day';
    else if (currentRange === 'custom' && customStart && customEnd) {
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
                data: data,
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

// --- Logs ---
function updateLogs(logs) {
    const tbody = document.getElementById('logs-body');
    if (!logs || logs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="px-4 py-6 text-center text-slate-500">No log data available.</td></tr>';
        return;
    }
    tbody.innerHTML = logs.map(row => {
        const statusClass = row.status === 'BAHAYA' ? 'bg-red-500/20 text-red-400' :
                            row.status === 'PERINGATAN' ? 'bg-yellow-500/20 text-yellow-400' :
                            'bg-green-500/20 text-green-400';
        const suhu = row.suhu !== null ? `${row.suhu.toFixed(2)} °${row.unit}` : '—';
        return `<tr class="border-b border-slate-800 hover:bg-slate-800/50">
            <td class="px-4 py-2 text-slate-400 whitespace-nowrap">${new Date(row.time).toLocaleString()}</td>
            <td class="px-4 py-2 font-mono text-slate-300">${row.device_id}</td>
            <td class="px-4 py-2 text-slate-300">${suhu}</td>
            <td class="px-4 py-2"><span class="rounded-full px-2 py-0.5 text-xs font-semibold ${statusClass}">${row.status}</span></td>
        </tr>`;
    }).join('');
}

// --- Fetch & refresh ---
async function fetchData() {
    try {
        let url = `/api/data?range=${currentRange}`;
        if (customStart && customEnd) {
            url = `/api/data?start=${customStart}&end=${customEnd}`;
        }
        const res = await fetch(url);
        const data = await res.json();
        updateGauge(data.current);
        updateChart(data.history || []);
        allLogs = data.logs || [];
        updateLogs(filteredLogs());
    } catch (e) {
        console.error('Fetch error:', e);
    }
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

// --- Tab switching ---
function switchTab(tab) {
    document.getElementById('panel-gauge').classList.toggle('hidden', tab !== 'gauge');
    document.getElementById('panel-chart').classList.toggle('hidden', tab !== 'chart');
    document.getElementById('tab-gauge').className = `rounded-lg px-5 py-2 text-sm font-medium ${tab === 'gauge' ? 'bg-slate-700 text-white' : 'text-slate-400 hover:text-slate-200'}`;
    document.getElementById('tab-chart').className = `rounded-lg px-5 py-2 text-sm font-medium ${tab === 'chart' ? 'bg-slate-700 text-white' : 'text-slate-400 hover:text-slate-200'}`;
}

// --- Range buttons ---
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
    fetchData();
}

// --- Date picker ---
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
    fetchData();
}

// --- Init ---
fetchData();
setInterval(fetchData, 30000); // Poll less frequently, MQTT handles live updates

// --- MQTT WebSocket for live gauge ---
const mqttClient = mqtt.connect('ws://43.159.61.247:9001');
mqttClient.on('connect', () => {
    mqttClient.subscribe('brake/temperature');
    console.log('🟢 MQTT connected');
});
mqttClient.on('message', (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());
        const deviceId = topic.split('/').pop();
        updateGauge({
            device_id: deviceId,
            suhu: payload.suhu,
            unit: payload.unit || 'C',
            status: payload.status || 'AMAN',
            time: new Date().toISOString(),
        });
    } catch (e) {}
});
