/* Clawbot Dashboard — multi-cycle CSV data viewer */

// --- CSV parser (no dependencies) ---
function parseCSV(text) {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map(h => h.trim());
  return lines.slice(1).map(line => {
    const values = line.split(",").map(v => v.trim());
    const row = {};
    headers.forEach((h, i) => { row[h] = values[i] || ""; });
    return row;
  });
}

function num(val) {
  const n = parseFloat(val);
  return isNaN(n) ? 0 : n;
}

function money(val) {
  return "$" + num(val).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

function commas(val) {
  return num(val).toLocaleString("en-US");
}

// --- Chart colors ---
const CYCLE_COLORS = {
  "2020": "#a78bfa",
  "2022": "#fbbf24",
  "2024": "#4f8cff",
  "2026": "#34d399",
};
const COLORS = [
  "#4f8cff", "#34d399", "#fbbf24", "#f87171", "#a78bfa",
  "#fb923c", "#38bdf8", "#f472b6", "#818cf8", "#2dd4bf",
];

const CHART_GRID = { color: "#2a2d3a" };
const CHART_TICK = { color: "#8b8fa3" };
function moneyAxis(v) {
  if (Math.abs(v) >= 1e6) return "$" + (v / 1e6).toFixed(1) + "M";
  if (Math.abs(v) >= 1e3) return "$" + (v / 1e3).toFixed(0) + "K";
  return "$" + v;
}

// --- Load data and render ---
async function init() {
  let rows;
  try {
    const resp = await fetch("data/district_summary.csv");
    if (!resp.ok) throw new Error(resp.status);
    rows = parseCSV(await resp.text());
  } catch (e) {
    document.getElementById("subtitle").textContent =
      "No data yet \u2014 set your Google Drive env vars on Render and redeploy.";
    return;
  }

  if (!rows.length) {
    document.getElementById("subtitle").textContent = "district_summary.csv is empty.";
    return;
  }

  // --- Discover cycles and districts ---
  const cycles = [...new Set(rows.map(r => r.cycle))].sort();
  const districts = [...new Set(rows.map(r => r.district))].sort();

  document.getElementById("subtitle").textContent =
    `Cycles: ${cycles.join(", ")} \u2014 ${districts.length} districts tracked`;

  // --- Summary cards (all cycles combined) ---
  const totalIndivCount = rows.reduce((s, r) => s + num(r.individual_donation_count), 0);
  const totalRaised = rows.reduce((s, r) => s + num(r.total_raised), 0);
  const totalSpent = rows.reduce((s, r) => s + num(r.expenditure_total), 0);

  document.getElementById("card-cycles").textContent = cycles.length;
  document.getElementById("card-districts").textContent = districts.length;
  document.getElementById("card-raised").textContent = money(totalRaised);
  document.getElementById("card-spent").textContent = money(totalSpent);

  // --- Build lookup: rows by (district, cycle) ---
  const lookup = {};
  rows.forEach(r => { lookup[r.district + "|" + r.cycle] = r; });

  // --- Fundraising by district, stacked by cycle ---
  const datasets = cycles.map(cyc => ({
    label: cyc,
    data: districts.map(d => {
      const r = lookup[d + "|" + cyc];
      return r ? num(r.total_raised) : 0;
    }),
    backgroundColor: CYCLE_COLORS[cyc] || COLORS[cycles.indexOf(cyc) % COLORS.length],
  }));

  new Chart(document.getElementById("raisedChart"), {
    type: "bar",
    data: { labels: districts, datasets },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: "#8b8fa3" } } },
      scales: {
        x: { stacked: true, ticks: CHART_TICK, grid: CHART_GRID },
        y: { stacked: true, ticks: { ...CHART_TICK, callback: moneyAxis }, grid: CHART_GRID },
      },
    },
  });

  // --- Trend chart: total raised per cycle across all districts ---
  const cycleTotals = cycles.map(cyc =>
    rows.filter(r => r.cycle === cyc).reduce((s, r) => s + num(r.total_raised), 0)
  );

  new Chart(document.getElementById("trendChart"), {
    type: "line",
    data: {
      labels: cycles,
      datasets: [{
        label: "Total Raised (all districts)",
        data: cycleTotals,
        borderColor: "#4f8cff",
        backgroundColor: "rgba(79, 140, 255, 0.15)",
        fill: true,
        tension: 0.3,
        pointRadius: 6,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: "#8b8fa3" } } },
      scales: {
        x: { ticks: CHART_TICK, grid: CHART_GRID },
        y: { ticks: { ...CHART_TICK, callback: moneyAxis }, grid: CHART_GRID },
      },
    },
  });

  // --- Table ---
  renderTable(rows);
}

// --- Sortable table ---
let sortCol = "district";
let sortAsc = true;

function renderTable(rows) {
  const tbody = document.querySelector("#summary-table tbody");
  const numCols = [
    "cycle", "individual_donation_count", "individual_donation_total",
    "pac_donation_count", "pac_donation_total",
    "expenditure_count", "expenditure_total",
    "total_raised", "net_cash_flow",
  ];

  const sorted = [...rows].sort((a, b) => {
    const av = numCols.includes(sortCol) ? num(a[sortCol]) : a[sortCol];
    const bv = numCols.includes(sortCol) ? num(b[sortCol]) : b[sortCol];
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  tbody.innerHTML = sorted.map(r => {
    const net = num(r.net_cash_flow);
    const netClass = net >= 0 ? "positive" : "negative";
    return `<tr>
      <td>${r.district}</td>
      <td>${r.cycle}</td>
      <td>${commas(r.individual_donation_count)}</td>
      <td>${money(r.individual_donation_total)}</td>
      <td>${commas(r.pac_donation_count)}</td>
      <td>${money(r.pac_donation_total)}</td>
      <td>${commas(r.expenditure_count)}</td>
      <td>${money(r.expenditure_total)}</td>
      <td>${money(r.total_raised)}</td>
      <td class="${netClass}">${money(r.net_cash_flow)}</td>
    </tr>`;
  }).join("");

  // Wire up sort on header clicks
  document.querySelectorAll("#summary-table th[data-col]").forEach(th => {
    th.onclick = () => {
      const col = th.dataset.col;
      if (sortCol === col) { sortAsc = !sortAsc; }
      else { sortCol = col; sortAsc = true; }
      renderTable(rows);
    };
  });
}

init();
