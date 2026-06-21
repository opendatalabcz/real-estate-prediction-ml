const map = L.map('map', { zoomControl: true }).setView([49.8, 15.5], 8);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

const markerLayer = L.featureGroup().addTo(map);
let currentMarker = null;
let boundaryFeatures = null;
let boundaryReady = false;
let pendingDetection = null;

function pointInAnyDistrict(lat, lng) {
  if (!boundaryReady) return null;
  const pt = turf.point([parseFloat(lng), parseFloat(lat)]);
  for (const feature of boundaryFeatures) {
    try {
      if (turf.booleanPointInPolygon(pt, feature)) {
        return feature.properties.district_id;
      }
    } catch(e) {}
  }
  return null;
}

function flashMessage(msg) {
  const el = document.getElementById('map-info');
  const orig = el.textContent;
  el.textContent = msg;
  el.style.background = '#d32f2f';
  el.style.color = '#fff';
  setTimeout(() => {
    el.textContent = orig;
    el.style.background = '';
    el.style.color = '';
  }, 2000);
}

function setMarker(lat, lng) {
  if (currentMarker) markerLayer.removeLayer(currentMarker);
  currentMarker = L.marker([lat, lng], { draggable: true }).addTo(markerLayer);
  currentMarker.on('dragend', function() {
    const pos = this.getLatLng();
    const lat = pos.lat.toFixed(5);
    const lng = pos.lng.toFixed(5);
    const did = pointInAnyDistrict(lat, lng);
    if (!did) {
      flashMessage('⛔ Bod musí být v ČR');
      markerLayer.removeLayer(this);
      currentMarker = null;
      return;
    }
    this.setLatLng([lat, lng]);
    updateLatLng(lat, lng);
  });
}

window.movePinTo = function(lat, lng) {
  const fLat = parseFloat(lat).toFixed(5);
  const fLng = parseFloat(lng).toFixed(5);
  const did = pointInAnyDistrict(fLat, fLng);
  if (!did) {
    flashMessage('⛔ Bod musí být v ČR');
    return;
  }
  setMarker(fLat, fLng);
  updateLatLng(fLat, fLng);
};

map.on('click', function(e) {
  const lat = e.latlng.lat.toFixed(5);
  const lng = e.latlng.lng.toFixed(5);
  const did = pointInAnyDistrict(lat, lng);
  if (!did) {
    flashMessage('⛔ Bod musí být v ČR');
    return;
  }
  setMarker(lat, lng);
  updateLatLng(lat, lng);
});

let districtCenters = {};
let regionCenters = {};

fetch('/api/boundaries')
  .then(r => r.json())
  .then(function(data) {
    boundaryFeatures = data.features;
    boundaryReady = true;

    // Compute interior center for each district (centroid, fallback to pointOnFeature)
    const regionPoints = {};
    data.features.forEach(f => {
      const did = f.properties.district_id;
      const district = DISTRICTS.find(d => d.district_id === did);
      if (!district) return;
      const rid = district.locality_region_id;
      try {
        let center = turf.centroid(f);
        if (!turf.booleanPointInPolygon(center, f)) {
          center = turf.pointOnFeature(f);
        }
        const lng = center.geometry.coordinates[0].toFixed(5);
        const lat = center.geometry.coordinates[1].toFixed(5);
        districtCenters[did] = [lat, lng];
        if (!regionPoints[rid]) regionPoints[rid] = [];
        regionPoints[rid].push([parseFloat(lng), parseFloat(lat)]);
      } catch(e) {}
    });
    // Compute region centers as average of their district centroids
    Object.keys(regionPoints).forEach(rid => {
      const pts = regionPoints[rid];
      const avgLat = pts.reduce((s, p) => s + p[1], 0) / pts.length;
      const avgLng = pts.reduce((s, p) => s + p[0], 0) / pts.length;
      regionCenters[rid] = [avgLat.toFixed(5), avgLng.toFixed(5)];
    });

    L.geoJSON(data, {
      style: { color: '#666', weight: 0.8, fillOpacity: 0 },
      interactive: false,
    }).addTo(map);
    if (pendingDetection) {
      detectDistrict(pendingDetection.lat, pendingDetection.lng);
      pendingDetection = null;
    }
  });

window.movePinToDistrict = function(districtId) {
  const center = districtCenters[districtId];
  if (!center) return;
  window.movePinTo(center[0], center[1]);
};

window.moveMapToRegion = function(regionId) {
  const center = regionCenters[regionId];
  if (!center) return;
  map.setView(center, 9);
};

function setAllLatLng(lat, lng) {
  const latEl = document.getElementById('latitude2');
  const lngEl = document.getElementById('longitude2');
  if (latEl) latEl.value = lat;
  if (lngEl) lngEl.value = lng;
}

function updateLatLng(lat, lng) {
  const info = document.getElementById('map-info');
  if (info) info.textContent = `📍 ${lat}, ${lng}`;
  setAllLatLng(lat, lng);
  if (window.updateCoordsFromMap) {
    window.updateCoordsFromMap(lat, lng);
  }
}

window.detectDistrictFromCoords = function(lat, lng) {
  detectDistrict(lat, lng);
};

function detectDistrict(lat, lng) {
  if (!boundaryReady) {
    pendingDetection = { lat, lng };
    return;
  }
  const pt = turf.point([parseFloat(lng), parseFloat(lat)]);
  for (const feature of boundaryFeatures) {
    try {
      if (turf.booleanPointInPolygon(pt, feature)) {
        const did = feature.properties.district_id;
        const district = DISTRICTS.find(d => d.district_id === did);
        if (district) {
          console.log(`📍 ${lat},${lng} → ${district.name} (${district.region_name})`);
          if (window.setDistrictValue) {
            window.setDistrictValue(did, district.locality_region_id);
          }
        }
        return;
      }
    } catch(e) {}
  }
}
