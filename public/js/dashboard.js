/* Clawbot Dashboard — reads CSV data and renders charts + table */

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
const COLORS = [
  "#4f8cff", "#34d399", "#fbbf24", "#f87171", "#a78bfa",
  "#fb923c", "#38bdf8", "#f472b6", "#818cf8", "#2dd4bf",
];

// --- Load data and render ---
async function init() {
  let rows;
  try {
    const resp = await fetch("data/district_summary.csv");
    if (!resp.ok) throw new Error(resp.status);
    rows = parseCSV(await resp.text());
  } catch (e) {
    document.getElementById("subtitle").textContent =
      "No data yet — the build pipeline has not run. Set your Google Drive env vars and redeploy.";
    return;
  }

  if (!rows.length) {
    document.getElementById("subtitle").textContent = "district_summary.csv is empty.";
    return;
  }

  // --- Summary cards ---
  const totalIndivCount = rows.reduce((s, r) => s + num(r.individual_donation_count), 0);
  const totalRaised = rows.reduce((s, r) => s + num(r.total_raised), 0);
  const totalSpent = rows.reduce((s, r) => s + num(r.expenditure_total), 0);

  document.getElementById("card-districts").textContent = rows.length;
  document.getElementById("card-indiv-count").textContent = commas(totalIndivCount);
  document.getElementById("card-raised").textContent = money(totalRaised);
  document.getElementById("card-spent").textContent = money(totalSpent);

  // --- Fundraising bar chart ---
  const labels = rows.map(r => r.district);
  const indivData = rows.map(r => num(r.individual_donation_total));
  const pacData = rows.map(r => num(r.pac_donation_total));

  new Chart(document.getElementById("raisedChart"), {
    type: "bar",
    data: {
      labels,
      datasets: [
        { label: "Individual", data: indivData, backgroundColor: "#4f8cff" },
        { label: "PAC", data: pacData, backgroundColor: "#34d399" },
      ],
    },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: "#8b8fa3" } } },
      scales: {
        x: { stacked: true, ticks: { color: "#8b8fa3" }, grid: { color: "#2a2d3a" } },
        y: {
          stacked: true,
          ticks: {
            color: "#8b8fa3",
            callback: v => "$" + (v >= 1e6 ? (v / 1e6).toFixed(1) + "M" : (v / 1e3).toFixed(0) + "K"),
          },
          grid: { color: "#2a2d3a" },
        },
      },
    },
  });

  // --- Spend vs raise scatter ---
  new Chart(document.getElementById("spendChart"), {
    type: "scatter",
    data: {
      datasets: [{
        label: "Districts",
        data: rows.map(r => ({
          x: num(r.total_raised),
          y: num(r.expenditure_total),
          label: r.district,
        })),
        backgroundColor: rows.map((_, i) => COLORS[i % COLORS.length]),
        pointRadius: 8,
      }],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => {
              const pt = ctx.raw;
              return `${pt.label}: Raised ${money(pt.x)}, Spent ${money(pt.y)}`;
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: "Total Raised", color: "#8b8fa3" },
          ticks: { color: "#8b8fa3", callback: v => "$" + (v / 1e6).toFixed(1) + "M" },
          grid: { color: "#2a2d3a" },
        },
        y: {
          title: { display: true, text: "Total Spent", color: "#8b8fa3" },
          ticks: { color: "#8b8fa3", callback: v => "$" + (v / 1e6).toFixed(1) + "M" },
          grid: { color: "#2a2d3a" },
        },
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
    "individual_donation_count", "individual_donation_total",
    "pac_donation_count", "pac_donation_total",
    "expenditure_count", "expenditure_total",
    "total_raised", "net_cash_flow",
  ];
  const moneyCols = [
    "individual_donation_total", "pac_donation_total",
    "expenditure_total", "total_raised", "net_cash_flow",
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
