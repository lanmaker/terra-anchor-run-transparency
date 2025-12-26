import Plotly from 'plotly.js-basic-dist';
import { csv } from 'd3-fetch';

function parseNumber(value) {
  const n = parseFloat(value);
  return Number.isFinite(n) ? n : 0;
}

function loadCsv(path) {
  return csv(path);
}

function buildSeries(rows, key) {
  return rows.map((row) => parseNumber(row[key]));
}

function buildTime(rows) {
  return rows.map((row) => new Date(row.hour));
}

async function main() {
  const mainContainer = document.querySelector('main');
  const loadingEl = document.createElement('div');
  loadingEl.className = 'loading';
  loadingEl.textContent = 'Loading data...';
  mainContainer.prepend(loadingEl);

  // Global error handler for unhandled promise rejections
  window.addEventListener('unhandledrejection', function (event) {
    console.error('Unhandled rejection (promise):', event.promise, 'reason:', event.reason);
    if (document.querySelector('.loading')) {
      document.querySelector('.loading').textContent = `Error: ${event.reason.message || event.reason}`;
      document.querySelector('.loading').classList.add('error');
    }
  });

  try {
    // Use relative path for compatibility with subfolders/IPFS/local
    const rows = await loadCsv("flows_hourly.csv");
    loadingEl.remove();
    const time = buildTime(rows);

    Plotly.newPlot(
      "net-outflow",
      [
        {
          x: time,
          y: buildSeries(rows, "net_outflow"),
          mode: "lines",
          line: { color: "#1b1f24" },
          name: "Net outflow",
        },
      ],
      {
        margin: { t: 20, r: 20, b: 40, l: 50 },
        xaxis: { title: "Hour" },
        yaxis: { title: "UST" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
      },
      { responsive: true }
    );

    Plotly.newPlot(
      "whale-vs-small",
      [
        {
          x: time,
          y: buildSeries(rows, "whale_outflow"),
          mode: "lines",
          line: { color: "#1f6f8b" },
          name: "Whale outflow",
        },
        {
          x: time,
          y: buildSeries(rows, "small_outflow"),
          mode: "lines",
          line: { color: "#d97706" },
          name: "Small outflow",
        },
      ],
      {
        margin: { t: 20, r: 20, b: 40, l: 50 },
        xaxis: { title: "Hour" },
        yaxis: { title: "UST" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        legend: { orientation: "h", y: 1.1 },
      },
      { responsive: true }
    );

    Plotly.newPlot(
      "concentration",
      [
        {
          x: time,
          y: buildSeries(rows, "hhi"),
          mode: "lines",
          line: { color: "#2ca02c" },
          name: "HHI",
        },
      ],
      {
        margin: { t: 20, r: 20, b: 40, l: 50 },
        xaxis: { title: "Hour" },
        yaxis: { title: "HHI" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
      },
      { responsive: true }
    );
  } catch (err) {
    console.error("Failed to load data:", err);
    if (document.querySelector('.loading')) {
      document.querySelector('.loading').textContent = 'Error loading data. Please check your connection.';
      document.querySelector('.loading').classList.add('error');
    }
  }
}

main();
