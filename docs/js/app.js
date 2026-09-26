let tempChart = null;

async function loadData() {
    const statusBar = document.getElementById("status-bar");
    const lastUpdated = document.getElementById("last-updated");
    try {
        const response = await fetch("data/latest.json?" + Date.now());
        if (!response.ok) throw new Error("No data yet");
        const data = await response.json();
        const when = new Date(data.generated_at).toLocaleString();
        lastUpdated.textContent = `Forecast generated: ${when} · ${data.cities.length} rows`;
        statusBar.classList.add("ok");
        renderToday(data.cities);
        renderMap(data.cities);
        renderChart(data.cities);
    } catch (err) {
        statusBar.classList.add("error");
        lastUpdated.textContent = "Waiting for the next pipeline run…";
        console.error(err);
    }
}

function formatNumber(v) {
    if (v === null || v === undefined) return "—";
    return Number(v).toFixed(1);
}

function renderToday(cities) {
    const tbody = document.getElementById("today-body");
    tbody.innerHTML = "";
    const today = new Date().toISOString().slice(0, 10);
    const todayRows = cities.filter(c => c.date === today);
    const rows = todayRows.length > 0 ? todayRows : cities.slice(0, 6);
    rows.forEach(c => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td><strong>${c.city_id}</strong></td>
            <td>${c.temperature_max ?? "—"}°C</td>
            <td>${c.temperature_min ?? "—"}°C</td>
            <td>${c.precipitation_sum ?? "—"} mm</td>
            <td>${c.wind_speed_max ?? "—"} km/h</td>
            <td>${formatNumber(c.pm2_5)}</td>
            <td>${formatNumber(c.pm10)}</td>
            <td>${formatNumber(c.nitrogen_dioxide)}</td>
            <td>${formatNumber(c.ozone)}</td>`;
        tbody.appendChild(tr);
    });
}

function renderMap(cities) {
    if (typeof L === "undefined") return;
    const map = L.map("map").setView([58.6, 25.0], 7);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "© OpenStreetMap contributors"
    }).addTo(map);
    const seen = new Set();
    cities.forEach(c => {
        if (seen.has(c.city_id)) return;
        seen.add(c.city_id);
        L.marker([c.latitude, c.longitude]).addTo(map)
            .bindPopup(`<strong>${c.city_id}</strong>`);
    });
}

function renderChart(cities) {
    if (typeof Chart === "undefined") return;
    const grouped = {};
    cities.forEach(c => {
        if (!grouped[c.city_id]) grouped[c.city_id] = [];
        grouped[c.city_id].push({ date: c.date, temp: parseFloat(c.temperature_max) });
    });
    Object.values(grouped).forEach(arr => arr.sort((a, b) => a.date.localeCompare(b.date)));
    const dates = [...new Set(cities.map(c => c.date))].sort();
    const palette = ["#58a6ff", "#3fb950", "#f0883e", "#db61a2", "#a371f7", "#39c5cf"];
    const datasets = Object.entries(grouped).map(([city, arr], i) => ({
        label: city,
        data: dates.map(d => {
            const m = arr.find(x => x.date === d);
            return m ? m.temp : null;
        }),
        borderColor: palette[i % palette.length],
        backgroundColor: palette[i % palette.length] + "33",
        tension: 0.3,
    }));
    const ctx = document.getElementById("temp-chart");
    if (tempChart) tempChart.destroy();
    tempChart = new Chart(ctx, {
        type: "line",
        data: { labels: dates, datasets },
        options: {
            responsive: true,
            plugins: { legend: { labels: { color: "#e6edf3" } } },
            scales: {
                x: { ticks: { color: "#8b949e" }, grid: { color: "#30363d" } },
                y: { ticks: { color: "#8b949e" }, grid: { color: "#30363d" },
                     title: { display: true, text: "°C", color: "#8b949e" } },
            },
        },
    });
}

loadData();
