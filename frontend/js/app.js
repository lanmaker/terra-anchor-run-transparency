/* global Plotly */

function parseNumber(value) {
  const n = parseFloat(value);
  return Number.isFinite(n) ? n : 0;
}

function loadCsv(path) {
  return new Promise((resolve, reject) => {
    Plotly.d3.csv(path, (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(rows);
    });
  });
}

function buildSeries(rows, key) {
  return rows.map((row) => parseNumber(row[key]));
}

function buildTime(rows) {
  return rows.map((row) => new Date(row.hour));
}

async function main() {
  try {
    const rows = await loadCsv("assets/flows_hourly.csv");
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
  }
}

main();
