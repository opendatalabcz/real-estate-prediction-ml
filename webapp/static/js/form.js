// --- SHAP help toggle ---
document.getElementById('shap-help-toggle')?.addEventListener('click', function() {
  const help = document.getElementById('shap-help');
  if (help) help.style.display = help.style.display === 'none' ? 'block' : 'none';
});

// --- Feature name translation: raw ML names → human-readable Czech ---
const FEATURE_NAMES_CS = {
  'district_id': 'Okres',
  'locality_region_id': 'Kraj',
  'building_condition': 'Stav budovy',
  'longitude': 'Zeměpisná délka',
  'usable_area_m2': 'Užitná plocha',
  'latitude': 'Zeměpisná šířka',
  'total_area_m2': 'Celková plocha',
  'has_elevator': 'Výtah',
  'ownership_type': 'Vlastnictví',
  'construction_type': 'Konstrukce',
  'total_floors': 'Počet pater',
  'has_terrace': 'Terasa',
  'category_sub': 'Dispozice',
  'is_furnished': 'Zařízeno',
  'location_type': 'Poloha v obci',
  'total_area_m2_was_missing': 'Plocha dopočtena',
  'has_cellar': 'Sklep',
  'energy_class': 'Energetická třída',
  'floor_number': 'Podlaží',
  'has_garage': 'Garáž',
  'cellar_area_m2': 'Plocha sklepa',
  'loggia_area_m2': 'Plocha lodžie',
  'has_loggia': 'Lodžie',
  'construction_year': 'Rok výstavby',
  'floor_ratio': 'Poměr podlaží',
  'is_top_floor': 'Nejvyšší podlaží',
  'building_age': 'Stáří budovy',
  'area_per_room': 'Plocha na místnost',
  'region_category': 'Kraj a dispozice',
  'region_condition': 'Kraj a stav budovy',
  'has_construction_year': 'Známý rok výstavby',
  'transport_nearest_km': 'Vzdálenost k MHD',
  'transport_count_within_500m': 'Zastávek MHD do 500m',
  'transport_count_within_1000m': 'Zastávek MHD do 1km',
  'metro_train_nearest_km': 'Vzdálenost k metru/vlaku',
  'grocery_nearest_km': 'Vzdálenost k obchodu',
  'grocery_count_within_500m': 'Obchodů do 500m',
  'grocery_count_within_1000m': 'Obchodů do 1km',
  'distance_to_city_center_km': 'Vzdálenost k centru kraje',
  'supermarket_nearest_km': 'Vzdálenost k supermarketu',
  'supermarket_count_within_500m': 'Supermarketů do 500m',
  'supermarket_count_within_1000m': 'Supermarketů do 1km',
  'convenience_nearest_km': 'Vzdálenost k večerce',
  'convenience_count_within_500m': 'Večerek do 500m',
  'convenience_count_within_1000m': 'Večerek do 1km',
  'transport_avg_3_nearest_km': 'Prům. vzdálenost 3 nejbl. zastávek',
  'grocery_avg_3_nearest_km': 'Prům. vzdálenost 3 nejbl. obchodů',
  'transport_quality_score': 'Kvalita dopravního spojení',
  'unique_poi_types_1000m': 'Druhů dopravy do 1km',
  'unique_poi_types_2000m': 'Druhů dopravy do 2km',
  'transport_density_sigma700m': 'Hustota dopravy (σ=700m)',
  'grocery_density_sigma700m': 'Hustota obchodů (σ=700m)',
  'has_transport_1km': 'MHD do 1km',
  'has_grocery_1km': 'Obchod do 1km',
};

function translateFeature(name) {
  return FEATURE_NAMES_CS[name] || name.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
}

// --- Toggle chips for amenities + conditional fields ---
document.querySelectorAll('.toggle').forEach(el => {
  el.addEventListener('click', function() {
    const nowOn = !this.classList.contains('on');
    this.classList.toggle('on');
    // Show/hide conditional field if this toggle has one
    const showId = this.dataset.show;
    if (showId) {
      const field = document.getElementById(showId);
      if (field) field.style.display = nowOn ? 'block' : 'none';
    }
  });
});

// --- District filtering ---
const regionEl = document.getElementById('region');
const districtEl = document.getElementById('district');

regionEl.addEventListener('change', function() {
  const regionId = parseInt(this.value);
  districtEl.innerHTML = '<option value="">Vyberte okres</option>';
  if (!regionId) return;
  DISTRICTS.filter(d => d.locality_region_id === regionId).forEach(d => {
    const opt = document.createElement('option');
    opt.value = d.district_id;
    opt.textContent = d.name;
    districtEl.appendChild(opt);
  });
  // Center map on region
  if (window.moveMapToRegion) window.moveMapToRegion(regionId);
  // Clear lat/lng fields
  ['latitude2', 'longitude2'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
});

window.setDistrictValue = function(districtId, regionId) {
  if (String(regionEl.value) !== String(regionId)) {
    regionEl.value = regionId;
    regionEl.dispatchEvent(new Event('change'));
  }
  requestAnimationFrame(() => {
    const opt = districtEl.querySelector(`option[value="${districtId}"]`);
    if (opt) districtEl.value = String(districtId);
  });
};

// When user manually selects a district in the dropdown, move pin
districtEl.addEventListener('change', function() {
  const did = parseInt(this.value);
  if (did && window.movePinToDistrict) window.movePinToDistrict(did);
});

function setLatLng(lat, lng) {
  const latEl = document.getElementById('latitude2');
  const lngEl = document.getElementById('longitude2');
  if (latEl) latEl.value = lat;
  if (lngEl) lngEl.value = lng;
}

window.updateCoordsFromMap = function(lat, lng) {
  setLatLng(lat, lng);
  if (window.detectDistrictFromCoords) {
    window.detectDistrictFromCoords(lat, lng);
  }
};

// Manual lat/lng changes on map card → move pin + detect district
['latitude2', 'longitude2'].forEach(id => {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('change', function() {
    const lat = document.getElementById('latitude2')?.value;
    const lng = document.getElementById('longitude2')?.value;
    if (lat && lng && !isNaN(parseFloat(lat)) && !isNaN(parseFloat(lng))) {
      if (window.movePinTo) window.movePinTo(lat, lng);
    }
  });
});

// --- Form submission ---
document.getElementById('predict-form').addEventListener('submit', async function(e) {
  e.preventDefault();

  // Validate: usable area ≤ total area
  const usable = parseFloat(document.getElementById('usable_area_m2')?.value) || 0;
  const total = parseFloat(document.getElementById('total_area_m2')?.value) || 0;
  if (usable > 0 && total > 0 && usable > total) {
    showError('Užitná plocha nemůže být větší než celková plocha.');
    return;
  }

  // Validate: floor number ≤ total floors
  const floor = parseFloat(document.getElementById('floor_number')?.value) || 0;
  const totFloors = parseFloat(document.getElementById('total_floors')?.value) || 0;
  if (floor > 0 && totFloors > 0 && floor > totFloors) {
    showError('Podlaží nemůže být vyšší než celkový počet pater.');
    return;
  }

  const btn = this.querySelector('.btn-primary');
  btn.disabled = true;
  btn.textContent = '⏳ Výpočet...';

  const payload = {};
  const fd = new FormData(this);
  fd.forEach((v, k) => { payload[k] = v; });

  document.querySelectorAll('.toggle.on').forEach(el => {
    payload[el.dataset.field] = true;
  });

  ['has_elevator','has_terrace','has_garage','has_cellar','has_loggia'].forEach(f => {
    if (!(f in payload)) payload[f] = false;
  });

  payload.is_furnished = payload.is_furnished || 'false';
  payload.construction_year = payload.construction_year || null;
  payload.loggia_area_m2 = payload.loggia_area_m2 || 0;
  payload.cellar_area_m2 = payload.cellar_area_m2 || 0;
  payload.floor_number = payload.floor_number || null;
  payload.total_floors = payload.total_floors || null;
  payload.locality_region_id = parseInt(payload.locality_region_id) || '';
  payload.district_id = parseInt(payload.district_id) || '';
  payload.latitude = parseFloat(document.getElementById('latitude2')?.value || 0);
  payload.longitude = parseFloat(document.getElementById('longitude2')?.value || 0);

  document.getElementById('result').style.display = 'none';
  document.getElementById('shap-section').style.display = 'none';
  document.getElementById('error').style.display = 'none';

  try {
    const resp = await fetch('/predict', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await resp.json();
    if (data.success) {
      document.getElementById('result-price').textContent = formatPrice(data.predicted_price_czk) + ' Kč';
      document.getElementById('result-per-m2').textContent = formatPrice(data.price_per_m2) + ' Kč / m²';
      const confEl = document.getElementById('result-confidence');
      if (data.confidence_lower_czk && data.confidence_upper_czk) {
        confEl.innerHTML = formatPrice(data.confidence_lower_czk) + ' – ' + formatPrice(data.confidence_upper_czk) + ' <span style="font-weight:400">Kč</span>';
        confEl.style.display = '';
      } else {
        confEl.style.display = 'none';
      }
      document.getElementById('result').style.display = 'block';
      requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));

      if (data.shap_values && data.shap_values.length > 0) {
        document.getElementById('shap-section').style.display = 'block';
        renderShap(data.shap_values, data.base_value_czk, data.predicted_price_czk);
      }
    } else {
      showError(data.error || 'Chyba při výpočtu');
    }
  } catch (err) {
    showError('Chyba připojení: ' + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = '🔮 Odhadnout cenu';
  }
});

const PLOTLY_CDN = 'https://cdn.plot.ly/plotly-3.0.1.min.js';
let plotlyReady = null;

function loadPlotly() {
    if (plotlyReady) return plotlyReady;
    plotlyReady = new Promise((resolve, reject) => {
        if (window.Plotly) return resolve();
        const s = document.createElement('script');
        s.src = PLOTLY_CDN;
        s.onload = () => resolve();
        s.onerror = () => reject(new Error('Plotly CDN failed'));
        document.head.appendChild(s);
    });
    return plotlyReady;
}

// SHAP waterfall: multiplicative compounding in CZK
async function renderShap(shapValues, baseCzk, finalCzk) {
  const container = document.getElementById('shap-chart');
  container.innerHTML = '';

  // Build cumulative CZK values from multiplicative impacts
  const labels = ['Průměr (základ)'];
  const vals = [baseCzk];        // absolute base bar value
  const measures = ['absolute'];
  const pctLabels = [''];

  let running = baseCzk;
  shapValues.forEach(s => {
    const multiplier = 1 + s.pct_impact / 100;
    const newRunning = running * multiplier;
    const incrementCzk = newRunning - running;
    labels.push(translateFeature(s.feature));
    vals.push(incrementCzk);
    measures.push('relative');
    pctLabels.push((s.pct_impact >= 0 ? '+' : '') + s.pct_impact.toFixed(1) + '%');
    running = newRunning;
  });

  // No final total bar — line + annotation below mark the prediction

  try {
    await loadPlotly();
    Plotly.newPlot(container, [{
      type: 'waterfall',
      y: labels,
      x: vals,
      measure: measures,
      orientation: 'h',
      text: pctLabels,
      textposition: 'outside',
      connector: { line: { color: '#cac4d0', width: 1, dash: 'dot' } },
      increasing: { marker: { color: '#2e7d32' } },
      decreasing: { marker: { color: '#d32f2f' } },
      totals: { marker: { color: '#6750a4' } },
      hoverinfo: 'y+x+text',
      hovertemplate: '%{y}<br>%{text}<br>%{x:,.0f} Kč<extra></extra>',
    }], {
      margin: { t: 40, b: 40, l: 160, r: 130 },
      height: Math.max(300, labels.length * 42 + 80),
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { family: 'Roboto, sans-serif', size: 11, color: '#49454f' },
      xaxis: {
        title: { text: 'Cena (Kč)', standoff: 4 },
        tickformat: ',.0f',
        separatethousands: true,
      },
      yaxis: { autorange: 'reversed', automargin: true },
      showlegend: false,
      shapes: [{
        type: 'line', x0: finalCzk, x1: finalCzk, y0: 0, y1: 1, yref: 'paper',
        line: { color: '#6750a4', width: 2.5, dash: 'dot' },
      }],
      annotations: [{
        x: finalCzk, y: 0,
        text: 'Odhadovaná cena = ' + formatPrice(finalCzk) + ' Kč',
        showarrow: true, arrowhead: 2, ax: 50, ay: -35,
        font: { size: 11, color: '#6750a4', weight: 700 },
        bgcolor: '#ffffff', bordercolor: '#cac4d0', borderwidth: 1, borderpad: 4,
      }],
    }, { responsive: true, displayModeBar: false }).then(() => {
      requestAnimationFrame(() => Plotly.Plots.resize(container));
    });
  } catch(e) {
    container.innerHTML = '<div style="padding:12px;color:var(--md-on-surface-variant)">Graf není dostupný</div>';
  }
}

function formatPrice(num) { return Math.round(num).toLocaleString('cs-CZ'); }
function showError(msg) {
  document.getElementById('error-msg').textContent = msg;
  document.getElementById('error').style.display = 'block';
}
