/* ============================================================
   FoodPulse — Dashboard JavaScript
   Chart.js 4.x · Dark Theme · Auto-refresh 30s
   ============================================================ */

// ── Chart.js Global Defaults ───────────────────────────────────
Chart.defaults.color = '#334155'; // Light mode default text
Chart.defaults.borderColor = 'rgba(0,0,0,0.06)'; // Light mode grid
Chart.defaults.font.family = 'Inter';
Chart.defaults.font.size = 12;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 10;
Chart.defaults.plugins.legend.labels.padding = 18;
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(255,255,255,0.95)';
Chart.defaults.plugins.tooltip.titleColor = '#0f172a';
Chart.defaults.plugins.tooltip.bodyColor = '#334155';
Chart.defaults.plugins.tooltip.borderColor = 'rgba(0,0,0,0.1)';
Chart.defaults.plugins.tooltip.borderWidth = 1;
Chart.defaults.plugins.tooltip.padding = 12;
Chart.defaults.plugins.tooltip.cornerRadius = 10;
Chart.defaults.plugins.tooltip.titleFont = { weight: '600' };

// ── Color Palette ──────────────────────────────────────────────
const COLORS = {
  cyan: '#06b6d4',
  purple: '#8b5cf6',
  green: '#10b981',
  red: '#ef4444',
  orange: '#f59e0b',
  blue: '#3b82f6',
  pink: '#ec4899',
  slate: '#94a3b8',
};

// ── Helpers ────────────────────────────────────────────────────
/** Format number as Indonesian Rupiah: Rp 15.000 */
function fmt(n, prefix) {
  if (n == null || isNaN(n)) return '—';
  prefix = prefix ?? 'Rp ';
  return prefix + Number(n).toLocaleString('id-ID', { maximumFractionDigits: 0 });
}

/** Format with decimals */
function fmtDec(n, digits) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('id-ID', { minimumFractionDigits: digits || 2, maximumFractionDigits: digits || 2 });
}

/** Short month label from date-like string or year/month ints */
function monthLabel(year, month, dateStr) {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];
  if (year && month) {
    return months[month - 1] + ' ' + year;
  }
  if (!dateStr) return '—';
  const dt = new Date(dateStr);
  if (isNaN(dt)) return String(dateStr).slice(0, 7);
  return months[dt.getMonth()] + ' ' + dt.getFullYear();
}

/** Safe fetch wrapper with graceful fallback */
async function safeFetch(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (e) {
    console.warn(`[FoodPulse] Fetch failed: ${url}`, e.message);
    return null;
  }
}

/** Animate a number from current to target */
function animateNumber(el, target, formatter) {
  if (!el) return;
  const current = parseFloat(el.dataset.value) || 0;
  const diff = target - current;
  const duration = 600;
  const start = performance.now();
  formatter = formatter || ((v) => fmt(v));

  function step(ts) {
    const elapsed = ts - start;
    const progress = Math.min(elapsed / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3); // ease-out cubic
    const val = current + diff * eased;
    el.textContent = formatter(val);
    if (progress < 1) requestAnimationFrame(step);
    else el.dataset.value = target;
  }
  requestAnimationFrame(step);
}

/** Create gradient for Chart.js line chart */
function createGradient(ctx, color, alpha1, alpha2) {
  const gradient = ctx.createLinearGradient(0, 0, 0, ctx.canvas.clientHeight);
  gradient.addColorStop(0, color.replace(')', `, ${alpha1 || 0.3})`).replace('rgb', 'rgba'));
  gradient.addColorStop(1, color.replace(')', `, ${alpha2 || 0.01})`).replace('rgb', 'rgba'));
  return gradient;
}

/** Hex to rgba */
function hexAlpha(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

// ── Chart Instances ────────────────────────────────────────────
let chartKurs = null;
let chartMonthly = null;
let chartForecast = null;
let chartCorrelation = null;

// ── Clock & Countdown ──────────────────────────────────────────
let refreshCountdown = 30;

function updateClock() {
  const now = new Date();
  const hh = String(now.getHours()).padStart(2, '0');
  const mm = String(now.getMinutes()).padStart(2, '0');
  const ss = String(now.getSeconds()).padStart(2, '0');
  const el = document.getElementById('liveClock');
  if (el) el.textContent = `${hh}:${mm}:${ss} WIB`;
}

function tickCountdown() {
  refreshCountdown--;
  if (refreshCountdown < 0) refreshCountdown = 30;
  const el = document.getElementById('refreshCountdown');
  if (el) el.textContent = refreshCountdown;
}

// ── Data Loaders ───────────────────────────────────────────────

/* ── Section 1 + 2: Live Kurs ─────────────────────────── */
async function loadKurs() {
  const data = await safeFetch('/api/kurs');
  if (!data) return;

  const history = data.history || (Array.isArray(data) ? data : []);
  const currentVal = data.current_rate || (history.length > 0 ? (history[history.length - 1].rate || history[history.length - 1].kurs_jual) : 0);
  const currentOil = data.current_oil || (history.length > 0 ? history[history.length - 1].oil_price_usd : 76.2);

  // 1. Update Kartu Kurs USD-IDR
  animateNumber(document.getElementById('metricKurs'), currentVal, (v) => fmt(v, 'Rp '));

  const subKurs = document.getElementById('metricKursSub');
  if (subKurs) {
    if (history.length >= 2) {
      const prevVal = history[history.length - 2].rate || history[history.length - 2].kurs_jual;
      const diff = currentVal - prevVal;
      const arrow = diff >= 0 ? '▲' : '▼';
      const cls = diff >= 0 ? 'arrow-up' : 'arrow-down';
      const diffText = Math.abs(diff) >= 0.1 ? `${arrow} ${fmt(Math.abs(diff), '')}` : 'stabil';
      subKurs.innerHTML = `<span class="${cls}">${diffText}</span> • spot interbank`;
    } else {
      subKurs.textContent = 'spot valas interbank';
    }
  }

  // 2. Update Kartu Minyak Mentah Brent (Dipisahkan Mandiri)
  const oilEl = document.getElementById('metricOil');
  if (oilEl && currentOil) {
    animateNumber(oilEl, currentOil, (v) => `$${Number(v).toFixed(2)}/bbl`);
  }

  const subOil = document.getElementById('metricOilSub');
  if (subOil) {
    if (history.length >= 2) {
      const prevOil = history[history.length - 2].oil_price_usd;
      if (prevOil !== undefined && prevOil !== null && currentOil) {
        const diffOil = currentOil - prevOil;
        const arrowO = diffOil >= 0 ? '▲' : '▼';
        const clsO = diffOil >= 0 ? 'arrow-up' : 'arrow-down';
        const diffTextO = Math.abs(diffOil) >= 0.01 ? `${arrowO} $${Math.abs(diffOil).toFixed(2)}` : 'stabil';
        subOil.innerHTML = `<span class="${clsO}">${diffTextO}</span> • energi &amp; logistik`;
      } else {
        subOil.textContent = 'energi & logistik global';
      }
    } else {
      subOil.textContent = 'energi & logistik global';
    }
  }

  // Dual-Axis Chart
  if (history.length > 0) {
    const labels = history.map(d => d.date || d.tanggal || '');
    const values = history.map(d => d.rate || d.kurs_jual || d.value);
    const oilValues = history.map(d => d.oil_price_usd !== undefined && d.oil_price_usd !== null ? d.oil_price_usd : 76.2);
    renderKursChart(labels, values, oilValues);
  }
}

function renderKursChart(labels, values, oilValues) {
  const ctx = document.getElementById('chartKurs');
  if (!ctx) return;

  const gradient = ctx.getContext('2d').createLinearGradient(0, 0, 0, 320);
  gradient.addColorStop(0, hexAlpha(COLORS.cyan, 0.25));
  gradient.addColorStop(1, hexAlpha(COLORS.cyan, 0.01));

  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  const gridColor = isLight ? 'rgba(0,0,0,0.06)' : 'rgba(255,255,255,0.04)';

  const datasets = [
    {
      label: 'Kurs USD-IDR (Rp)',
      data: values,
      borderColor: COLORS.cyan,
      backgroundColor: gradient,
      borderWidth: 2.5,
      fill: true,
      tension: 0.35,
      pointRadius: 0,
      pointHoverRadius: 5,
      pointHoverBackgroundColor: COLORS.cyan,
      yAxisID: 'y',
    }
  ];

  if (oilValues && oilValues.some(v => v !== null)) {
    datasets.push({
      label: 'Brent Crude Oil ($/BBL)',
      data: oilValues,
      borderColor: '#f59e0b',
      borderWidth: 2,
      borderDash: [4, 4],
      fill: false,
      tension: 0.35,
      pointRadius: 0,
      pointHoverRadius: 5,
      pointHoverBackgroundColor: '#f59e0b',
      yAxisID: 'y1',
    });
  }

  const config = {
    type: 'line',
    data: {
      labels: labels.map(l => l.length > 10 ? l.slice(5) : l),
      datasets: datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          labels: {
            boxWidth: 12,
            font: { size: 11 },
            color: isLight ? '#475569' : '#94a3b8'
          }
        },
        tooltip: {
          callbacks: {
            label: (tip) => {
              if (tip.dataset.yAxisID === 'y1') {
                return ` Brent Oil: $${Number(tip.raw).toFixed(2)}/BBL`;
              }
              return ` Kurs: ${fmt(tip.raw)}`;
            },
          },
        },
      },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 8, font: { size: 11 } } },
        y: {
          type: 'linear',
          position: 'left',
          grid: { color: gridColor },
          ticks: { callback: v => fmt(v) }
        },
        y1: {
          type: 'linear',
          position: 'right',
          grid: { drawOnChartArea: false },
          ticks: { callback: v => `$${v}` }
        }
      },
    },
  };

  if (chartKurs) {
    chartKurs.data = config.data;
    chartKurs.options = config.options;
    chartKurs.update('none');
  } else {
    chartKurs = new Chart(ctx, config);
  }
}

/* ── Speed Layer: Live In-Flight Alerts ───────────────── */
async function loadLiveAlerts() {
  const data = await safeFetch('/api/live_alerts');
  const container = document.getElementById('liveAlertsContainer');
  const statusPill = document.getElementById('streamStatusPill');
  const statusDot = document.getElementById('streamStatusDot');
  const statusText = document.getElementById('streamStatusText');
  const tooltipTitle = document.getElementById('tooltipTitle');
  const tooltipDesc = document.getElementById('tooltipDesc');

  const alerts = data && Array.isArray(data.alerts) ? data.alerts : [];

  // Jika kondisi pasar normal (tidak ada kejutan pasar):
  if (alerts.length === 0) {
    if (container) {
      container.innerHTML = '';
      container.style.display = 'none';
    }
    if (statusPill) statusPill.className = 'stream-status-pill normal';
    if (statusDot) statusDot.className = 'status-dot green';
    if (statusText) statusText.textContent = 'Stream Normal';
    if (tooltipTitle) tooltipTitle.textContent = 'Speed Layer: Optimal';
    if (tooltipDesc) tooltipDesc.textContent = 'Aliran Kafka (9092) stabil. Tidak ada lonjakan kurs anomali atau pelanggaran HET aktif.';
    return;
  }

  // Jika ADA kejutan / shock pasar aktif:
  if (statusPill) statusPill.className = 'stream-status-pill alert';
  if (statusDot) statusDot.className = 'status-dot red pulse';
  if (statusText) statusText.textContent = `Alert (${alerts.length})`;
  if (tooltipTitle) tooltipTitle.textContent = `🚨 ${alerts.length} Peringatan Aktif`;
  if (tooltipDesc) tooltipDesc.textContent = alerts.map(a => a.title).join(' | ');

  if (!container) return;
  container.style.display = 'block';
  container.innerHTML = alerts.map(a => {
    const isDanger = a.type === 'danger';
    const borderColor = isDanger ? 'var(--accent-red)' : 'var(--accent-orange)';
    const badgeBg = isDanger ? 'rgba(239, 68, 68, 0.15)' : 'rgba(245, 158, 11, 0.15)';
    const badgeColor = isDanger ? 'var(--accent-red)' : 'var(--accent-orange)';
    const icon = isDanger ? '🚨' : '⚠️';

    return `
      <div class="glass-card fade-in" style="padding: 14px 18px; margin-bottom: 12px; border-left: 4px solid ${borderColor}; border-radius: var(--radius-md); box-shadow: 0 4px 20px rgba(0,0,0,0.12);">
        <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 14px; flex-wrap: wrap;">
          <div style="display: flex; align-items: flex-start; gap: 10px; flex: 1;">
            <span style="font-size: 1.3rem; line-height: 1.2;">${icon}</span>
            <div>
              <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 3px; flex-wrap: wrap;">
                <span style="font-size: 0.7rem; background: ${badgeBg}; color: ${badgeColor}; padding: 2px 7px; border-radius: 5px; font-weight: 700; letter-spacing: 0.5px;">${a.badge}</span>
                <span style="font-weight: 600; font-size: 0.92rem; color: var(--text-primary);">${a.title}</span>
              </div>
              <div style="font-size: 0.82rem; color: var(--text-secondary); margin-bottom: 5px;">${a.message}</div>
              <div style="font-size: 0.8rem; color: ${borderColor}; font-weight: 500;">💡 <b>Aksi Cepat:</b> ${a.action}</div>
            </div>
          </div>
          <div style="font-size: 0.72rem; color: var(--text-muted); white-space: nowrap;">${a.timestamp}</div>
        </div>
      </div>
    `;
  }).join('');
}

/* ── Section 2: Live Prices ───────────────────────────── */
async function loadPangan() {
  const data = await safeFetch('/api/pangan');
  if (!data) return;

  const container = document.getElementById('livePrices');
  if (!container) return;

  const items = data.items || (Array.isArray(data) ? data : []);
  if (items.length === 0) {
    container.innerHTML = '<div class="empty-state" style="padding: 24px; text-align: center; color: var(--text-muted); font-size: 0.85rem;">Belum ada aliran data harga pangan</div>';
    return;
  }

  // Group by komoditas
  const komoditasMap = {};
  items.forEach(item => {
    const name = item.komoditas || item.name || item.nama || 'Unknown';
    if (!komoditasMap[name]) komoditasMap[name] = item;
  });
  const latest = Object.values(komoditasMap);

  // Update average change metric
  const changes = latest.map(i => i.perubahan_pct || i.change_pct || 0).filter(c => !isNaN(c) && c !== 0);
  if (changes.length > 0) {
    const avg = changes.reduce((a, b) => a + b, 0) / changes.length;
    animateNumber(document.getElementById('metricChange'), avg, (v) => fmtDec(v, 2) + '%');
  } else {
    document.getElementById('metricChange').textContent = '—';
  }

  container.innerHTML = latest.map(item => {
    const name = item.komoditas || item.name || item.nama || '—';
    const price = item.harga || item.price || 0;
    const change = item.perubahan_pct || item.change_pct || 0;
    const changeDir = change > 0 ? 'up' : change < 0 ? 'down' : 'flat';
    const changeArrow = change > 0 ? '▲' : change < 0 ? '▼' : '—';
    const date = item.tanggal || item.date || '';
    const timeStr = date.includes(' ') ? date.split(' ')[1] : date;
    const kategori = item.kategori || 'Pangan Pokok';

    let catClass = 'badge-cat';
    const catLower = kategori.toLowerCase();
    if (catLower.includes('impor')) catClass += ' cat-impor';
    else if (catLower.includes('volatile') || catLower.includes('lokal')) catClass += ' cat-volatile';
    else if (catLower.includes('pokok') || catLower.includes('het')) catClass += ' cat-pokok';

    return `
      <div class="price-item-row fade-in">
        <div class="item-info">
          <div class="item-name">${name}</div>
          <div class="item-sub">
            <span class="${catClass}">${kategori}</span>
            <span class="item-time">⏱️ ${timeStr}</span>
          </div>
        </div>
        <div class="item-pricing">
          <div class="item-val">${fmt(price)}</div>
          <div class="item-badge-wrap">
            <span class="badge-change ${changeDir}">${change !== 0 ? `${changeArrow} ${fmtDec(Math.abs(change), 2)}%` : 'stabil'}</span>
          </div>
        </div>
      </div>`;
  }).join('');
}

/* ── Section 3: Monthly Trends ────────────────────────── */
async function loadMonthlyTrends() {
  const [kursData, panganData] = await Promise.all([
    safeFetch('/api/monthly_kurs'),
    safeFetch('/api/monthly_pangan'),
  ]);

  const ctx = document.getElementById('chartMonthly');
  if (!ctx) return;

  const datasets = [];
  let labels = [];

  // Kurs dataset (left Y axis)
  if (kursData && Array.isArray(kursData)) {
    labels = kursData.map(d => monthLabel(d.year, d.month, d.date || d.bulan));
    const gradient = ctx.getContext('2d').createLinearGradient(0, 0, 0, 360);
    gradient.addColorStop(0, hexAlpha(COLORS.cyan, 0.15));
    gradient.addColorStop(1, hexAlpha(COLORS.cyan, 0.01));

    datasets.push({
      label: 'Kurs USD-IDR',
      data: kursData.map(d => d.avg_kurs || d.kurs || d.value),
      borderColor: COLORS.cyan,
      backgroundColor: gradient,
      fill: true,
      borderWidth: 2.5,
      tension: 0.35,
      pointRadius: 3,
      pointHoverRadius: 6,
      pointBackgroundColor: COLORS.cyan,
      yAxisID: 'yKurs',
    });
  }

  // Pangan dataset(s) (right Y axis)
  if (panganData) {
    let grouped = {};
    if (Array.isArray(panganData)) {
      panganData.forEach(d => {
        const k = d.komoditas || d.nama || 'Pangan';
        if (!grouped[k]) grouped[k] = [];
        grouped[k].push(d);
      });
    } else {
      grouped = panganData; // It's already grouped like {"Beras": [...], "Cabai": [...]}
    }

    const colorList = [COLORS.purple, COLORS.pink, COLORS.orange, COLORS.green, COLORS.blue];
    let ci = 0;
    for (const [name, rows] of Object.entries(grouped)) {
      const c = colorList[ci % colorList.length];
      if (labels.length === 0) labels = rows.map(d => monthLabel(d.year, d.month, d.date || d.bulan));
      datasets.push({
        label: name,
        data: rows.map(d => d.avg_harga || d.harga || d.price || d.value),
        borderColor: c,
        backgroundColor: hexAlpha(c, 0.08),
        fill: false,
        borderWidth: 2,
        tension: 0.35,
        pointRadius: 2,
        pointHoverRadius: 5,
        pointBackgroundColor: c,
        yAxisID: 'yPangan',
      });
      ci++;
    }
  }

  const config = {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'top' } },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 12, font: { size: 11 } } },
        yKurs: {
          type: 'linear',
          position: 'left',
          title: { display: true, text: 'Kurs (IDR)', color: COLORS.cyan },
          grid: { color: document.documentElement.getAttribute('data-theme') === 'light' ? 'rgba(0,0,0,0.06)' : 'rgba(255,255,255,0.04)' },
          ticks: { callback: v => fmt(v), color: COLORS.cyan },
        },
        yPangan: {
          type: 'linear',
          position: 'right',
          title: { display: true, text: 'Harga (IDR)', color: COLORS.purple },
          grid: { drawOnChartArea: false },
          ticks: { callback: v => fmt(v), color: COLORS.purple },
        },
      },
    },
  };

  if (chartMonthly) { chartMonthly.data = config.data; chartMonthly.update('none'); }
  else { chartMonthly = new Chart(ctx, config); }
}

/* ── Section 4: ML Forecasting ────────────────────────── */
async function loadForecast() {
  const data = await safeFetch('/api/forecast');
  if (!data) return;

  const predEl = document.getElementById('predictionCards');
  if (predEl && data.forecasts) {
    const preds = Array.isArray(data.forecasts) ? data.forecasts : [];
    if (preds.length === 0) {
      predEl.innerHTML = '<div class="glass-card forecast-natural-card empty-state">Tidak ada data prediksi tersedia</div>';
      return;
    }

    predEl.innerHTML = preds.map(p => {
      const name = p.komoditas || p.nama || '—';
      const weeks = p.predictions || [];
      const currentPrice = p.current_price || (weeks.length > 0 ? weeks[0].predicted_price : 0);
      const w4 = weeks.length >= 4 ? weeks[3] : (weeks[weeks.length - 1] || { predicted_price: currentPrice });
      const targetPrice = w4.predicted_price;
      const delta = targetPrice - currentPrice;
      const deltaPct = currentPrice > 0 ? ((delta / currentPrice) * 100) : 0;

      // Natural Trend Badge
      let trendBadge = '';
      if (Math.abs(deltaPct) < 0.15) {
        trendBadge = `<span class="trend-badge flat">● Stabil</span>`;
      } else if (deltaPct > 0) {
        trendBadge = `<span class="trend-badge up">▲ +${fmtDec(deltaPct, 1)}%</span>`;
      } else {
        trendBadge = `<span class="trend-badge down">▼ -${fmtDec(Math.abs(deltaPct), 1)}%</span>`;
      }

      // Natural descriptive note
      let changeNote = '';
      if (Math.abs(delta) < 20) {
        changeNote = `Cenderung stabil (selisih ${fmt(Math.abs(delta))}) dalam 4 minggu ke depan.`;
      } else if (delta > 0) {
        changeNote = `Diproyeksikan naik +${fmt(delta)} dalam 4 minggu ke depan.`;
      } else {
        changeNote = `Diproyeksikan turun -${fmt(Math.abs(delta))} dalam 4 minggu ke depan.`;
      }

      // Minimalist weekly progression: horizontal flow
      const timelineHtml = weeks.map((w, idx) => {
        const isFinal = idx === weeks.length - 1;
        return `
          <div class="weekly-dot-step ${isFinal ? 'target-step' : ''}">
            <span class="dot-label">M${w.week}</span>
            <span class="dot-price">${fmt(w.predicted_price)}</span>
          </div>`;
      }).join('<span class="weekly-connector">›</span>');

      return `
        <div class="glass-card forecast-natural-card fade-in">
          <div class="card-head-simple">
            <div class="card-title-simple">${name}</div>
            ${trendBadge}
          </div>

          <div class="forecast-hero-price">
            <span class="hero-val">${fmt(targetPrice)}</span>
            <span class="hero-basis">Basis: ${fmt(currentPrice)}</span>
          </div>

          <div class="forecast-note-simple">${changeNote}</div>

          <div class="weekly-strip-wrap">
            ${timelineHtml}
          </div>

          <div class="card-footer-meta">
            <span>PySpark Multi-Variate</span>
            <span>Akurasi Model R² <b>${p.r2_score ?? '—'}</b></span>
          </div>
        </div>`;
    }).join('');
  }
}

/* ── Section 5: Anomaly Detection ─────────────────────── */
async function loadAnomalies() {
  const data = await safeFetch('/api/anomaly');
  if (!data) return;

  const items = Array.isArray(data) ? data : (data.anomalies || []);
  const container = document.getElementById('anomalyTimeline');
  if (!container) return;

  // Update metric
  animateNumber(document.getElementById('metricAnomaly'), items.length, (v) => Math.round(v).toString());
  const subEl = document.getElementById('metricAnomalySub');
  if (subEl) {
    const highCount = items.filter(i => (i.severity || '').toUpperCase() === 'HIGH').length;
    subEl.innerHTML = highCount > 0
      ? `<span class="pulse-dot"></span> ${highCount} severity tinggi`
      : 'deteksi anomali harga';
  }

  if (items.length === 0) {
    container.innerHTML = '<div class="glass-card anomaly-card empty-state">Tidak ada anomali terdeteksi</div>';
    return;
  }

  container.innerHTML = items.map(a => {
    const sev = (a.severity || 'LOW').toUpperCase();
    const deviation = a.deviation_pct || a.deviation || 0;
    const cause = a.cause || a.possible_cause || a.reason || '';

    return `
      <div class="glass-card anomaly-card severity-${sev} fade-in">
        <div class="anomaly-header">
          <span class="anomaly-komoditas">${a.komoditas || a.nama || '—'}</span>
          <span class="severity-badge ${sev}">${sev}</span>
        </div>
        <div class="anomaly-deviation">${a.deviasi_persen > 0 ? '+' : ''}${fmtDec(a.deviasi_persen, 1)}%</div>
        <div class="anomaly-meta">
          ${a.bulan || a.tanggal || a.date || ''} · Harga: ${fmt(a.harga_aktual || a.harga || a.price)} · Normal: ${fmt(a.harga_expected || a.harga_normal || a.expected_price)}
        </div>
        ${(a.kemungkinan_penyebab || cause) ? `<div class="anomaly-cause">${a.kemungkinan_penyebab || cause}</div>` : ''}
      </div>`;
  }).join('');
}

/* ── Section 6: HET Early Warning ─────────────────────── */
async function loadHETWarning() {
  const data = await safeFetch('/api/het_warning');
  if (!data) return;

  const items = Array.isArray(data) ? data : (data.warnings || []);
  const container = document.getElementById('hetGrid');
  if (!container) return;

  // Update metric
  const violations = items.filter(i => {
    const s = (i.status || '').toUpperCase();
    return s.includes('KRITIS') || s.includes('DARURAT');
  }).length;
  animateNumber(document.getElementById('metricHET'), violations, (v) => Math.round(v).toString());
  const subEl = document.getElementById('metricHETSub');
  if (subEl) {
    subEl.innerHTML = violations > 0
      ? `<span class="pulse-dot"></span> ${violations} komoditas di atas HET`
      : 'semua komoditas di bawah HET';
  }

  if (items.length === 0) {
    container.innerHTML = '<div class="glass-card het-natural-card empty-state">Semua harga di bawah HET</div>';
    return;
  }

  container.innerHTML = items.map(h => {
    const name = h.komoditas || h.nama || '—';
    const pct = h.persen_dari_het || h.pct_of_het || h.percentage || 0;
    const price = h.harga_terakhir || h.harga || h.current_price || 0;
    const het = h.het || h.het_limit || 0;
    const satuan = h.satuan || 'kg';
    const projected = h.proyeksi_jebol_het || h.proyeksi_tembus_het || h.projected_breach;
    const weeksToBreach = h.estimasi_minggu_jebol || h.estimasi_minggu_tembus;

    // Natural status & badge
    let statusBadge = '';
    let barColorClass = 'safe';
    let headroomText = '';

    if (price > het) {
      const breach = price - het;
      statusBadge = `<span class="het-pill danger">🚨 Melebihi HET (${fmtDec(pct, 0)}%)</span>`;
      barColorClass = 'danger';
      headroomText = `<span class="note-danger">Melampaui batas sebesar <b>${fmt(breach)}/${satuan}</b></span>`;
    } else if (pct >= 98) {
      const diff = het - price;
      statusBadge = `<span class="het-pill warning">⚠️ Kritis (${fmtDec(pct, 1)}%)</span>`;
      barColorClass = 'warning';
      headroomText = `<span class="note-warning">Hampir menyentuh batas (sisa ${fmt(diff)}/${satuan})</span>`;
    } else if (pct >= 90) {
      const diff = het - price;
      statusBadge = `<span class="het-pill watch">Waspada (${fmtDec(pct, 1)}%)</span>`;
      barColorClass = 'watch';
      headroomText = `<span class="note-watch">Sisa batas aman: <b>${fmt(diff)}/${satuan}</b></span>`;
    } else {
      const diff = het - price;
      statusBadge = `<span class="het-pill safe">Aman (${fmtDec(pct, 1)}%)</span>`;
      barColorClass = 'safe';
      headroomText = `<span class="note-safe">Sisa batas aman: <b>${fmt(diff)}/${satuan}</b></span>`;
    }

    const barWidth = Math.min(Math.max(pct, 0), 100);

    return `
      <div class="glass-card het-natural-card fade-in">
        <div class="card-head-simple">
          <div class="card-title-simple">${name}</div>
          ${statusBadge}
        </div>

        <div class="het-price-row">
          <span class="het-curr-val">${fmt(price)}</span>
          <span class="het-ceiling-val">Pagu HET: ${fmt(het)}/${satuan}</span>
        </div>

        <div class="het-progress-simple">
          <div class="het-bar-thin">
            <div class="het-bar-fill-simple ${barColorClass}" style="width: ${barWidth}%;"></div>
          </div>
        </div>

        <div class="het-headroom-simple">
          ${headroomText}
          ${projected && weeksToBreach ? `<span class="het-breach-alert">⚠️ Risiko tembus ${weeksToBreach} mgg</span>` : ''}
        </div>
      </div>`;
  }).join('');
}

/* ── Section 7: Correlation ───────────────────────────── */
async function loadCorrelation() {
  const data = await safeFetch('/api/correlation');
  if (!data) return;

  const items = Array.isArray(data) ? data : (data.by_komoditas || data.correlations || []);
  const ctx = document.getElementById('chartCorrelation');
  if (!ctx || items.length === 0) return;

  const labels = items.map(d => d.komoditas || d.nama || '—');
  const corrKurs = items.map(d => d.pearson_corr_kurs ?? d.pearson_corr ?? d.corr_kurs ?? 0);
  const corrOil = items.map(d => d.pearson_corr_oil ?? d.corr_oil ?? 0);

  const config = {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'vs Kurs USD',
          data: corrKurs,
          backgroundColor: hexAlpha(COLORS.cyan, 0.7),
          borderColor: COLORS.cyan,
          borderWidth: 1,
          borderRadius: 6,
          barPercentage: 0.55,
        },
        {
          label: 'vs Oil Price',
          data: corrOil,
          backgroundColor: hexAlpha(COLORS.orange, 0.7),
          borderColor: COLORS.orange,
          borderWidth: 1,
          borderRadius: 6,
          barPercentage: 0.55,
        },
      ],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'top' } },
      scales: {
        x: {
          min: -1,
          max: 1,
          grid: { color: document.documentElement.getAttribute('data-theme') === 'light' ? 'rgba(0,0,0,0.06)' : 'rgba(255,255,255,0.04)' },
          ticks: { callback: v => v.toFixed(1) },
          title: { display: true, text: 'Pearson r', color: COLORS.slate },
        },
        y: {
          grid: { display: false },
          ticks: { font: { size: 12 } },
        },
      },
    },
  };

  if (chartCorrelation) { chartCorrelation.data = config.data; chartCorrelation.update('none'); }
  else { chartCorrelation = new Chart(ctx, config); }
}

/* ── Section 8: FPVI Analytics Table ──────────────────── */
async function loadAnalytics() {
  const data = await safeFetch('/api/analytics');
  if (!data) return;

  const items = Array.isArray(data) ? data : (data.komoditas || data.analytics || []);
  const tbody = document.getElementById('analyticsBody');
  if (!tbody) return;

  if (items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state">Belum ada data FPVI</td></tr>';
    return;
  }

  tbody.innerHTML = items.map((row, idx) => {
    const komoditas = row.komoditas || row.nama || '—';
    const bulan = '4 Minggu ke Depan';
    const avgHarga = row.harga_terakhir || row.avg_harga || row.harga || 0;
    const avgKurs = data.kurs_terakhir || row.avg_kurs || 0;
    const fpvi = row.fpvi_score || row.fpvi || row.vulnerability_index || 0;
    const kategori = row.fpvi_level || row.kategori || row.category || '—';
    const lagInfo = row.peak_transmission_lag ? `⏱️ <b>Transmisi Shock:</b> Dampak transmisi makro berada pada <b>${row.peak_transmission_lag}</b>.` : '';
    const corrInfo = (row.corr_kurs && row.corr_oil) ? ` Korelasi Kurs: <b>${row.corr_kurs}</b> | Minyak: <b>${row.corr_oil}</b>.` : '';
    const insight = row.insight || (lagInfo + corrInfo);

    // Color for kategori
    let katClass = 'text-green';
    const katUpper = kategori.toUpperCase();
    if (katUpper.includes('TINGGI') || katUpper.includes('HIGH') || katUpper.includes('KRITIS')) katClass = 'text-red';
    else if (katUpper.includes('SEDANG') || katUpper.includes('MEDIUM') || katUpper.includes('WASPADA')) katClass = 'text-orange';

    return `
      <tr onclick="this.classList.toggle('expanded')" title="Klik untuk detail">
        <td><span class="expand-icon">▶</span></td>
        <td>${komoditas}</td>
        <td>${bulan}</td>
        <td>${fmt(avgHarga)}</td>
        <td>${fmt(avgKurs)}</td>
        <td><strong>${fmtDec(fpvi, 2)}</strong></td>
        <td><span class="${katClass}" style="font-weight:600">${kategori}</span></td>
      </tr>
      ${insight ? `<tr class="insight-row" style="display:none"><td colspan="7"><div class="insight-text" style="display:block">${insight}</div></td></tr>` : ''}`;
  }).join('');

  // Handle expandable rows
  tbody.querySelectorAll('tr[onclick]').forEach(tr => {
    tr.addEventListener('click', function () {
      const next = this.nextElementSibling;
      if (next && next.classList.contains('insight-row')) {
        next.style.display = next.style.display === 'none' ? '' : 'none';
      }
    });
  });
}

/* ── Telemetry: Live Cluster Health (Formal & Unified) ── */
async function loadPipelineHealth() {
  const data = await safeFetch('/api/pipeline_health');
  if (!data) return;

  const valStatus = document.getElementById('valSystemStatus');
  const dotStatus = document.getElementById('dotSystemStatus');

  if (valStatus && dotStatus) {
    const kafkaUp = data.kafka?.status === 'UP';
    const hdfsUp = data.hdfs?.status === 'UP';

    if (kafkaUp && hdfsUp) {
      valStatus.textContent = 'Klaster & Streaming Operasional';
      dotStatus.className = 'status-dot green';
    } else if (kafkaUp || hdfsUp) {
      valStatus.textContent = 'Klaster Berjalan Sebagian (Degraded)';
      dotStatus.className = 'status-dot orange';
    } else {
      valStatus.textContent = 'Mode Penyajian Lokal (Offline Stream)';
      dotStatus.className = 'status-dot orange';
    }
  }
}

// ── Master Refresh ─────────────────────────────────────────────
async function refreshAll() {
  refreshCountdown = 30;
  await Promise.allSettled([
    loadLiveAlerts(),
    loadPipelineHealth(),
    loadKurs(),
    loadPangan(),
    loadMonthlyTrends(),
    loadForecast(),
    loadAnomalies(),
    loadHETWarning(),
    loadCorrelation(),
    loadAnalytics(),
  ]);
}

// ── Init ───────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  updateClock();
  setInterval(updateClock, 1000);
  setInterval(tickCountdown, 1000);

  // Theme Toggle Logic
  const themeToggle = document.getElementById('themeToggle');
  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const root = document.documentElement;
      const currentTheme = root.getAttribute('data-theme');
      const newTheme = currentTheme === 'light' ? 'dark' : 'light';
      root.setAttribute('data-theme', newTheme);

      // Update Chart Colors
      const textColor = newTheme === 'light' ? '#334155' : '#94a3b8';
      const gridColor = newTheme === 'light' ? 'rgba(0,0,0,0.06)' : 'rgba(255,255,255,0.04)';

      Chart.defaults.color = textColor;
      if (chartKurs) {
        chartKurs.options.scales.y.grid.color = gridColor;
        chartKurs.update('none');
      }
      if (chartMonthly) {
        chartMonthly.options.scales.yKurs.grid.color = gridColor;
        chartMonthly.update('none');
      }
      if (chartCorrelation) {
        chartCorrelation.options.scales.x.grid.color = gridColor;
        chartCorrelation.update('none');
      }
    });
  }

  refreshAll();
  setInterval(refreshAll, 30000);
});