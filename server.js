require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { InfluxDB } = require('@influxdata/influxdb-client');
const path = require('path');
const app = express();

// ==================== DATABASE CONFIGURATIONS ====================

// InfluxDB Configuration - UPDATED with your actual values
const INFLUX_CONFIG = {
  url: process.env.INFLUX_URL || 'https://us-east-1-1.aws.cloud2.influxdata.com',
  token: process.env.INFLUX_TOKEN,
  org: process.env.INFLUX_ORG || 'SmartPanchayat',  // Changed from 'Smart Panchayat'
  bucket: process.env.INFLUX_BUCKET || 'sensor_data'  // Changed from 'smart_panchayat'
};

const PORT = process.env.PORT || 8181;

// Initialize InfluxDB clients
const influxDB = new InfluxDB({ url: INFLUX_CONFIG.url, token: INFLUX_CONFIG.token });
const queryApi = influxDB.getQueryApi(INFLUX_CONFIG.org);

// MySQL Configuration - Using your railway credentials
const mysql = require('mysql2');

console.log('🔧 Configuration:');
console.log('   InfluxDB Bucket:', INFLUX_CONFIG.bucket);
console.log('   InfluxDB Org:', INFLUX_CONFIG.org);
console.log('   MySQL Host:', process.env.MYSQL_HOST || 'switchback.proxy.rlwy.net');
console.log('   MySQL Port:', process.env.MYSQL_PORT || '59975');
console.log('   MySQL Database:', process.env.MYSQL_DATABASE || 'railway');

let db;
if (process.env.MYSQL_URL) {
  console.log('   Using MYSQL_URL connection');
  db = mysql.createPool({
    uri: process.env.MYSQL_URL,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  }).promise();
} else {
  db = mysql.createPool({
    host: process.env.MYSQL_HOST || 'switchback.proxy.rlwy.net',
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE || 'railway',
    port: parseInt(process.env.MYSQL_PORT) || 59975,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  }).promise();
}

// Test MySQL connection immediately
(async () => {
  try {
    await db.query('SELECT 1');
    console.log('✅ MySQL database connected successfully');
    
    await db.query(`
      CREATE TABLE IF NOT EXISTS system_settings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        setting_key VARCHAR(100) UNIQUE NOT NULL,
        setting_value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    
    try {
      await db.query(`
        ALTER TABLE sensors 
        ADD COLUMN IF NOT EXISTS type VARCHAR(50) DEFAULT 'general' AFTER name
      `);
      console.log('✅ Sensors table ready');
    } catch (error) {
      console.log('ℹ️ Note:', error.message);
    }
    
  } catch (error) {
    console.error('❌ MySQL connection failed:', error.message);
  }
})();

// ==================== OTP STORE ====================
const otpStore = new Map();

const VILLAGER_WITH_LOCATION_SQL = `
  SELECT
    v.id,
    v.name,
    v.phone,
    v.address,
    v.panchayat_id,
    p.name AS panchayat_name,
    p.state AS panchayat_state,
    parent.name AS parent_name,
    parent.type AS parent_type,
    parent.state AS parent_state,
    grandparent.name AS grandparent_name,
    grandparent.type AS grandparent_type,
    grandparent.state AS grandparent_state
  FROM villagers v
  LEFT JOIN locations p ON p.id = v.panchayat_id
  LEFT JOIN locations parent ON parent.id = p.parent_id
  LEFT JOIN locations grandparent ON grandparent.id = parent.parent_id
`;

const SENSOR_WITH_LOCATION_SQL = `
  SELECT
    s.id AS sensor_id,
    s.name AS sensor_name,
    s.type AS sensor_type,
    s.status AS sensor_status,
    s.location_description,
    s.villager_id,
    s.panchayat_id,
    s.district_id,
    s.installed_at,
    s.updated_at,
    p.name AS panchayat_name,
    p.state AS panchayat_state,
    parent.name AS parent_name,
    parent.type AS parent_type,
    parent.state AS parent_state,
    grandparent.name AS grandparent_name,
    grandparent.type AS grandparent_type,
    grandparent.state AS grandparent_state,
    d.name AS district_name,
    d.state AS district_state
  FROM sensors s
  LEFT JOIN locations p ON p.id = s.panchayat_id
  LEFT JOIN locations parent ON parent.id = p.parent_id
  LEFT JOIN locations grandparent ON grandparent.id = parent.parent_id
  LEFT JOIN locations d ON d.id = s.district_id
`;

function extractVillageLabel(address, fallback = '') {
  if (!address) return fallback;
  const firstPart = String(address)
    .split(',')
    .map(part => part.trim())
    .find(Boolean);
  return firstPart || fallback;
}

function resolveDistrictName(row) {
  if (row.district_name) return row.district_name;
  if (row.parent_type === 'district') return row.parent_name || '';
  if (row.grandparent_type === 'district') return row.grandparent_name || '';
  return '';
}

function resolveStateName(row) {
  return (
    row.district_state ||
    row.panchayat_state ||
    row.parent_state ||
    row.grandparent_state ||
    'Kerala'
  );
}

function normalizeVillager(row) {
  const panchayat = row.panchayat_name || '';
  const district = resolveDistrictName(row);
  const state = resolveStateName(row);
  const village = extractVillageLabel(row.address, panchayat);

  return {
    id: row.id,
    name: row.name,
    phone: row.phone,
    address: row.address || '',
    village,
    panchayat,
    district,
    state,
    role: 'villager'
  };
}

function normalizeSensorLocation(row) {
  const panchayat = row.panchayat_name || '';
  const district = resolveDistrictName(row);
  const state = resolveStateName(row);
  const village = row.location_description || panchayat || '';

  return {
    village,
    panchayat,
    district,
    state
  };
}

async function fetchVillagerByPhone(phone, conn = db) {
  const [rows] = await conn.query(
    `${VILLAGER_WITH_LOCATION_SQL} WHERE v.phone = ?`,
    [phone]
  );

  if (rows.length === 0) {
    return null;
  }

  return normalizeVillager(rows[0]);
}

function getUnitForType(type) {
  const units = {
    'temperature': '°C',
    'humidity': '%',
    'pressure': 'hPa',
    'water_level': '%',
    'water_quality': 'pH',
    'air_quality': 'AQI',
    'ph': 'pH',
    'gas': 'ppm',
    'turbidity': 'NTU',
    'chlorine': 'mg/L',
    'water': 'L',
    'air': 'μg/m³'
  };
  return units[type] || '';
}

async function fetchLatestSensorSnapshot(sensorId, sensorType) {
  const dataQuery = `
    from(bucket: "${INFLUX_CONFIG.bucket}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> filter(fn: (r) => r.devEUI == "${sensorId}")
      |> last()
  `;

  const data = await queryInfluxDB(dataQuery);

  let measurement = 'No data';
  let time = '';
  let status = 'Offline';
  let numericValue = null;

  if (data && data.length > 0) {
    const point = data[0];
    const readingTime = new Date(point._time);
    time = readingTime.toLocaleString();
    const diffSeconds = (Date.now() - readingTime.getTime()) / 1000;
    status = diffSeconds <= 22 ? 'Live' : 'Offline';
    
    // Try different methods to extract value
    let value = null;
    
    if (point[sensorType] !== undefined) {
      value = point[sensorType];
    }
    else if (point.value !== undefined) {
      value = point.value;
    }
    else if (point._value !== undefined) {
      value = point._value;
    }
    else {
      for (let key in point) {
        if (!key.startsWith('_') && typeof point[key] === 'number') {
          value = point[key];
          break;
        }
      }
    }
    
    if (value !== null && value !== undefined) {
      const numValue = typeof value === 'number' ? value : parseFloat(value);
      if (!isNaN(numValue)) {
        numericValue = numValue;
        const unit = getUnitForType(sensorType);
        measurement = `${sensorType}: ${numValue.toFixed(2)} ${unit}`;
      }
    }
    
    if (measurement === 'No data' && point.measurement) {
      measurement = point.measurement;
    }
  }

  return { measurement, time, status, numericValue };
}

async function buildSensorPayload(row, { includeAssignment = true } = {}) {
  const location = normalizeSensorLocation(row);
  const latest = await fetchLatestSensorSnapshot(row.sensor_id, row.sensor_type);

  return {
    id: row.sensor_id,
    devEUI: row.sensor_id,
    name: row.sensor_name,
    type: row.sensor_type,
    village: location.village,
    panchayat: location.panchayat,
    district: location.district,
    state: location.state,
    measurement: latest.measurement,
    time: latest.time,
    status: latest.status,
    ...(includeAssignment ? { isAssigned: row.villager_id !== null } : {})
  };
}

// ==================== MIDDLEWARE ====================

app.use(cors({
  origin: '*',
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept', 'Origin', 'X-Requested-With']
}));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  res.header('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.json());

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  if (req.body && Object.keys(req.body).length > 0) {
    console.log('Body:', JSON.stringify(req.body));
  }
  next();
});

// ==================== HELPER FUNCTIONS ====================

async function queryInfluxDB(fluxQuery) {
  try {
    const result = await queryApi.collectRows(fluxQuery);
    return result || [];
  } catch (error) {
    console.error('❌ InfluxDB query error:', error.message);
    return [];
  }
}

const SENSOR_ACTIVE_THRESHOLD = 20;

async function getActiveSensorCount() {
  const query = `
    from(bucket: "${INFLUX_CONFIG.bucket}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> group(columns: ["devEUI"])
      |> last()
      |> keep(columns: ["devEUI", "_time"])
  `;

  const rows = await queryInfluxDB(query);
  const now = Date.now();

  return rows.filter(r => {
    const diffSeconds = (now - new Date(r._time).getTime()) / 1000;
    return diffSeconds <= SENSOR_ACTIVE_THRESHOLD;
  }).length;
}

function determineSensorType(name) {
    const lowerName = name.toLowerCase();
    if (lowerName.includes('temperature')) return 'temperature';
    if (lowerName.includes('humidity')) return 'humidity';
    if (lowerName.includes('pressure')) return 'pressure';
    if (lowerName.includes('water') && lowerName.includes('level')) return 'water_level';
    if (lowerName.includes('water') && lowerName.includes('quality')) return 'water_quality';
    if (lowerName.includes('water')) return 'water';
    if (lowerName.includes('ph')) return 'ph';
    if (lowerName.includes('air') && lowerName.includes('quality')) return 'air_quality';
    if (lowerName.includes('air')) return 'air';
    if (lowerName.includes('gas')) return 'gas';
    if (lowerName.includes('turbidity')) return 'turbidity';
    if (lowerName.includes('chlorine')) return 'chlorine';
    return 'general';
}

// ==================== API ROUTES ====================

app.get('/api/test', (req, res) => {
  res.json({
    success: true,
    message: 'API is working!',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/health', async (req, res) => {
  try {
    await db.query('SELECT 1');
    
    res.json({
      success: true,
      message: 'Smart Panchayat Backend is running',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      databases: {
        mysql: 'connected',
        influxdb: 'connected'
      },
      config: {
        influxBucket: INFLUX_CONFIG.bucket,
        influxOrg: INFLUX_CONFIG.org
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Database connection failed',
      message: error.message
    });
  }
});

// Test InfluxDB connection
app.get('/api/test-influx', async (req, res) => {
  try {
    const query = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> limit(n: 1)
    `;
    const result = await queryInfluxDB(query);
    res.json({ 
      success: true, 
      message: 'InfluxDB connected', 
      bucket: INFLUX_CONFIG.bucket,
      org: INFLUX_CONFIG.org,
      hasData: result.length > 0,
      sampleData: result[0] || null 
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ==================== VILLAGER MANAGEMENT ====================

app.get('/api/villagers', async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT id, aadhaar AS aadhaar_number, name, phone, village, panchayat, district, state, occupation, address, created_at
       FROM villagers ORDER BY created_at DESC`
    );

    res.json({ success: true, villagers: rows, count: rows.length });
  } catch (err) {
    console.error('❌ Error fetching villagers:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    const [rows] = await db.query(
      `SELECT id, aadhaar AS aadhaar_number, name, phone, village, panchayat, district, state, occupation, address, created_at
       FROM villagers WHERE aadhaar = ?`,
      [aadhaarNumber]
    );

    if (rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    res.json({ success: true, villager: rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/villagers', async (req, res) => {
  try {
    const { aadhaarNumber, name, phone, village, panchayat, district, state, occupation, address } = req.body;

    if (!aadhaarNumber || !name || !phone || !village || !panchayat || !district || !state) {
      return res.status(400).json({ 
        success: false, 
        error: 'Missing required fields: aadhaarNumber, name, phone, village, panchayat, district, state' 
      });
    }

    await db.query(
      `INSERT INTO villagers (aadhaar, name, phone, village, panchayat, district, state, occupation, address)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [aadhaarNumber, name, phone, village, panchayat, district, state, occupation, address]
    );

    res.json({ success: true, message: 'Villager added successfully' });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ success: false, error: 'Aadhaar or phone already exists' });
    }
    res.status(500).json({ success: false, error: err.message });
  }
});

app.put('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    const { name, phone, village, panchayat, district, state, occupation, address } = req.body;

    const [result] = await db.query(
      `UPDATE villagers SET name = ?, phone = ?, village = ?, panchayat = ?, district = ?, state = ?, occupation = ?, address = ?
       WHERE aadhaar = ?`,
      [name, phone, village, panchayat, district, state, occupation, address, aadhaarNumber]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    res.json({ success: true, message: 'Villager updated' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.delete('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    const [result] = await db.query(`DELETE FROM villagers WHERE aadhaar = ?`, [aadhaarNumber]);

    if (result.affectedRows === 0) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    res.json({ success: true, message: 'Villager deleted' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/villagers/:aadhaar/sensors', async (req, res) => {
  try {
    const { aadhaar } = req.params;

    const [[villager]] = await db.query(
      `SELECT id, name, aadhaar, phone, village, panchayat, district, state
       FROM villagers WHERE aadhaar = ?`,
      [aadhaar]
    );

    if (!villager) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    const [sensors] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s JOIN villager_sensors vs ON vs.sensor_id = s.id WHERE vs.villager_id = ?`,
      [villager.id]
    );

    const result = [];

    for (const sensor of sensors) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.devEUI, sensor.type);
      
      result.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        type: sensor.type,
        village: sensor.village,
        panchayat: sensor.panchayat,
        district: sensor.district,
        state: sensor.state,
        measurement: snapshot.measurement,
        time: snapshot.time,
        status: snapshot.status
      });
    }

    res.json({ success: true, villager, sensors: result });
  } catch (err) {
    console.error('❌ Error fetching villager sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== SENSOR MANAGEMENT ====================

// Get sensors by district
app.get('/api/sensors/by-district', async (req, res) => {
  try {
    const { district } = req.query;
    console.log('📍 Fetching sensors for district:', district);

    if (!district) {
      return res.status(400).json({
        success: false,
        error: 'District name is required'
      });
    }

    const [sensorRows] = await db.query(
      `${SENSOR_WITH_LOCATION_SQL}
       WHERE (
         d.name = ?
         OR (d.id IS NULL AND parent.type = 'district' AND parent.name = ?)
         OR (d.id IS NULL AND grandparent.type = 'district' AND grandparent.name = ?)
       )
       ORDER BY s.name ASC`,
      [district, district, district]
    );

    console.log(`📊 Found ${sensorRows.length} sensors in ${district} district`);

    const sensors = [];

    for (const sensorRow of sensorRows) {
      sensors.push(await buildSensorPayload(sensorRow));
    }

    res.json({
      success: true,
      district: district,
      sensors: sensors,
      count: sensors.length
    });

  } catch (err) {
    console.error('❌ Error fetching sensors by district:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Get district statistics
app.get('/api/district-stats/:district', async (req, res) => {
  try {
    const { district } = req.params;
    
    console.log(`📍 Fetching statistics for district: ${district}`);
    
    const [sensorRows] = await db.query(
      `${SENSOR_WITH_LOCATION_SQL}
       WHERE s.villager_id IS NULL
         AND (
           d.name = ?
           OR (d.id IS NULL AND parent.type = 'district' AND parent.name = ?)
           OR (d.id IS NULL AND grandparent.type = 'district' AND grandparent.name = ?)
         )
       ORDER BY s.type ASC, s.name ASC`,
      [district, district, district]
    );
    
    console.log(`📊 Found ${sensorRows.length} unmapped sensors in ${district}`);
    
    const sensorsByType = {};
    
    for (const sensor of sensorRows) {
      const sensorType = sensor.sensor_type;
      if (!sensorsByType[sensorType]) {
        sensorsByType[sensorType] = [];
      }
      sensorsByType[sensorType].push(sensor);
    }
    
    const stats = {};
    
    for (const [type, sensors] of Object.entries(sensorsByType)) {
      let total = 0;
      let validCount = 0;
      const sensorValues = [];
      
      for (const sensor of sensors) {
        try {
          const snapshot = await fetchLatestSensorSnapshot(
            sensor.sensor_id,
            sensor.sensor_type
          );
          const value = snapshot.numericValue;

          if (value !== null && !isNaN(value) && value > 0) {
            total += value;
            validCount++;
            sensorValues.push({
              name: sensor.sensor_name,
              village: normalizeSensorLocation(sensor).village,
              value: value
            });
          }
        } catch (err) {
          console.log(`Error fetching data for sensor ${sensor.sensor_id}:`, err.message);
        }
      }
      
      if (validCount > 0) {
        const average = total / validCount;
        stats[type] = {
          count: validCount,
          totalSensors: sensors.length,
          average: average,
          unit: getUnitForType(type),
          sensors: sensorValues,
          lastUpdated: new Date().toISOString()
        };
      } else {
        stats[type] = {
          count: 0,
          totalSensors: sensors.length,
          average: null,
          unit: getUnitForType(type),
          sensors: [],
          lastUpdated: null
        };
      }
    }
    
    res.json({
      success: true,
      district: district,
      stats: stats,
      timestamp: new Date().toISOString()
    });
    
  } catch (err) {
    console.error('❌ Error fetching district statistics:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Get single sensor
app.get('/api/sensors/:devEUI', async (req, res) => {
  try {
    const { devEUI } = req.params;

    const [rows] = await db.query(
      `${SENSOR_WITH_LOCATION_SQL}
       WHERE s.id = ?`,
      [devEUI]
    );

    if (rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }

    const sensor = await buildSensorPayload(rows[0]);
    res.json({ success: true, sensor });
  } catch (err) {
    console.error('❌ Error fetching sensor:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get all sensors
app.get('/api/sensors', async (req, res) => {
  try {
    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, type, village, panchayat, district, state
       FROM sensors ORDER BY id DESC`
    );

    const sensors = [];

    for (const sensor of sensorRows) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.devEUI, sensor.type);
      
      sensors.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        type: sensor.type,
        village: sensor.village,
        panchayat: sensor.panchayat,
        district: sensor.district,
        state: sensor.state,
        measurement: snapshot.measurement,
        time: snapshot.time,
        status: snapshot.status
      });
    }

    res.json({ success: true, sensors });
  } catch (err) {
    console.error('❌ Error fetching sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Add new sensor
app.post('/api/sensors', async (req, res) => {
  const { devEUI, deviceName, type, village, panchayat, district, state, phone } = req.body;

  if (!devEUI || !deviceName) {
    return res.status(400).json({ success: false, error: 'devEUI and deviceName are required' });
  }

  let sensorType = type || determineSensorType(deviceName);

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const [sensorResult] = await conn.query(
      `INSERT INTO sensors (devEUI, name, type, village, panchayat, district, state)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [devEUI, deviceName, sensorType, village || null, panchayat || null, district || null, state || null]
    );

    const sensorId = sensorResult.insertId;

    if (phone) {
      const [[villager]] = await conn.query(`SELECT id FROM villagers WHERE phone = ?`, [phone]);
      if (!villager) throw new Error('No villager found with this phone number');
      await conn.query(`INSERT INTO villager_sensors (villager_id, sensor_id) VALUES (?, ?)`, [villager.id, sensorId]);
    }

    await conn.commit();
    res.json({ success: true, message: phone ? 'Sensor registered and mapped to villager' : 'Sensor registered successfully' });
  } catch (err) {
    await conn.rollback();
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ success: false, error: 'Sensor already exists' });
    }
    res.status(400).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});

// Update sensor
app.put('/api/sensors/:devEUI', async (req, res) => {
  const { devEUI } = req.params;
  const { deviceName, type, village, panchayat, district, state, phone } = req.body;

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const [result] = await conn.query(
      `UPDATE sensors SET name = ?, type = ?, village = ?, panchayat = ?, district = ?, state = ? WHERE devEUI = ?`,
      [deviceName, type || 'general', village || null, panchayat || null, district || null, state || null, devEUI]
    );

    if (result.affectedRows === 0) throw new Error('Sensor not found');

    await conn.query(`DELETE FROM villager_sensors WHERE sensor_id = (SELECT id FROM sensors WHERE devEUI = ?)`, [devEUI]);

    if (phone) {
      const [[villager]] = await conn.query(`SELECT id FROM villagers WHERE phone = ?`, [phone]);
      if (!villager) throw new Error('Villager not found');
      await conn.query(`INSERT INTO villager_sensors (villager_id, sensor_id) VALUES (?, (SELECT id FROM sensors WHERE devEUI = ?))`, [villager.id, devEUI]);
    }

    await conn.commit();
    res.json({ success: true });
  } catch (err) {
    await conn.rollback();
    res.status(400).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});

// Delete sensor
app.delete('/api/sensors/:devEUI', async (req, res) => {
  const { devEUI } = req.params;

  if (!devEUI) return res.status(400).json({ success: false, error: 'devEUI is required' });

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const [[sensor]] = await conn.query(`SELECT id FROM sensors WHERE devEUI = ?`, [devEUI]);
    if (!sensor) return res.status(404).json({ success: false, error: 'Sensor not found' });

    await conn.query(`DELETE FROM villager_sensors WHERE sensor_id = ?`, [sensor.id]);
    await conn.query(`DELETE FROM sensors WHERE id = ?`, [sensor.id]);

    await conn.commit();
    res.json({ success: true, message: 'Sensor deleted successfully' });
  } catch (err) {
    await conn.rollback();
    res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});

// ==================== SENSOR HISTORY ENDPOINT ====================

app.get('/api/sensors/:devEUI/history', async (req, res) => {
  try {
    const { devEUI } = req.params;
    const { range } = req.query;
    let rangeValue = range ? range : '-24h';

    console.log(`📊 Fetching history for sensor ${devEUI}, range: ${rangeValue}`);

    const [sensorInfo] = await db.query(
      'SELECT type FROM sensors WHERE id = ?',
      [devEUI]
    );
    
    if (sensorInfo.length === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }
    
    const sensorType = sensorInfo[0].type;

    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: ${rangeValue})
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.devEUI == "${devEUI}")
        |> sort(columns: ["_time"], desc: false)
    `;

    const rows = await queryInfluxDB(fluxQuery);
    
    console.log(`📊 Found ${rows.length} raw records for sensor ${devEUI}`);
    
    if (rows.length === 0) {
      return res.json({ success: true, history: [], count: 0, message: 'No data found for this sensor' });
    }

    const history = [];
    
    for (const row of rows) {
      let value = null;
      let timestamp = row._time;
      
      if (row[sensorType] !== undefined) {
        value = typeof row[sensorType] === 'number' ? row[sensorType] : parseFloat(row[sensorType]);
      }
      else if (row.value !== undefined) {
        value = typeof row.value === 'number' ? row.value : parseFloat(row.value);
      }
      else if (row._value !== undefined) {
        value = typeof row._value === 'number' ? row._value : parseFloat(row._value);
      }
      else {
        for (let key in row) {
          if (!key.startsWith('_') && typeof row[key] === 'number' && key !== 'devEUI') {
            value = row[key];
            break;
          }
        }
      }
      
      if (value !== null && !isNaN(value) && isFinite(value)) {
        history.push({
          time: timestamp,
          value: value,
          field: sensorType
        });
      }
    }

    console.log(`✅ Returning ${history.length} valid numeric data points`);

    res.json({
      success: true,
      history: history,
      count: history.length,
      sensorType: sensorType,
      unit: getUnitForType(sensorType)
    });

  } catch (err) {
    console.error('❌ Error fetching sensor history:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== DISTRICT ENDPOINTS ====================

app.get('/api/districts', async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT DISTINCT name
       FROM locations
       WHERE type = 'district'
       ORDER BY name`
    );
    const districts = rows.map(row => row.name);
    res.json({ success: true, districts, count: districts.length });
  } catch (err) {
    console.error('❌ Error fetching districts:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/all-district-stats', async (req, res) => {
  try {
    const [districtRows] = await db.query(
      `SELECT DISTINCT name
       FROM locations
       WHERE type = 'district'
       ORDER BY name`
    );
    
    const allDistricts = districtRows.map(row => row.name);
    const allStats = {};
    
    for (const district of allDistricts) {
      const [sensorRows] = await db.query(
        `${SENSOR_WITH_LOCATION_SQL}
         WHERE s.villager_id IS NULL
           AND (
             d.name = ?
             OR (d.id IS NULL AND parent.type = 'district' AND parent.name = ?)
             OR (d.id IS NULL AND grandparent.type = 'district' AND grandparent.name = ?)
           )
         ORDER BY s.type ASC, s.name ASC`,
        [district, district, district]
      );
      
      const sensorsByType = {};
      
      for (const sensor of sensorRows) {
        const sensorType = sensor.sensor_type;
        if (!sensorsByType[sensorType]) {
          sensorsByType[sensorType] = [];
        }
        sensorsByType[sensorType].push(sensor);
      }
      
      const districtStats = {};
      
      for (const [type, sensors] of Object.entries(sensorsByType)) {
        let total = 0;
        let validCount = 0;
        
        const promises = sensors.map(async sensor => {
          const snapshot = await fetchLatestSensorSnapshot(
            sensor.sensor_id,
            sensor.sensor_type
          );
          return snapshot.numericValue;
        });
        
        const values = await Promise.all(promises);
        
        for (const value of values) {
          if (value !== null && !isNaN(value) && value > 0) {
            total += value;
            validCount++;
          }
        }
        
        if (validCount > 0) {
          districtStats[type] = {
            count: validCount,
            totalSensors: sensors.length,
            average: total / validCount,
            unit: getUnitForType(type),
          };
        } else {
          districtStats[type] = {
            count: 0,
            totalSensors: sensors.length,
            average: null,
            unit: getUnitForType(type),
          };
        }
      }
      
      allStats[district] = districtStats;
    }
    
    res.json({
      success: true,
      stats: allStats,
      timestamp: new Date().toISOString()
    });
    
  } catch (err) {
    console.error('❌ Error fetching all district stats:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== MOBILE ENDPOINTS ====================

app.get('/api/mobile/sensors', async (req, res) => {
  try {
    const { phone } = req.query;
    if (!phone) return res.status(400).json({ success: false, error: 'Phone number is required' });

    const villager = await fetchVillagerByPhone(phone);

    if (!villager) return res.status(404).json({ success: false, error: 'Villager not found' });

    const [sensorRows] = await db.query(
      `${SENSOR_WITH_LOCATION_SQL}
       WHERE s.villager_id = ?
       ORDER BY s.installed_at DESC, s.updated_at DESC, s.id DESC`,
      [villager.id]
    );

    const sensors = [];

    for (const sensorRow of sensorRows) {
      sensors.push(await buildSensorPayload(sensorRow, { includeAssignment: false }));
    }

    res.json({
      success: true,
      villager: {
        name: villager.name,
        phone: villager.phone,
        village: villager.village,
        panchayat: villager.panchayat,
        district: villager.district,
        state: villager.state
      },
      sensors,
      sensorCount: sensors.length
    });
  } catch (err) {
    console.error('❌ Error fetching mobile sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== ADMIN DASHBOARD ====================

app.get('/api/admin/dashboard', async (req, res) => {
  try {
    const [[{ totalVillagers }]] = await db.query(`SELECT COUNT(*) AS totalVillagers FROM villagers`);
    const totalSensors = await getActiveSensorCount();

    const [recentVillagers] = await db.query(
      `SELECT name, aadhaar AS aadhaar_number, village, panchayat, district, state, phone
       FROM villagers ORDER BY created_at DESC LIMIT 5`
    );

    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, type, village, panchayat, district, state FROM sensors ORDER BY installed_at DESC LIMIT 5`
    );

    const recentSensors = [];
    for (const sensor of sensorRows) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.devEUI, sensor.type);
      recentSensors.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        type: sensor.type,
        village: sensor.village,
        panchayat: sensor.panchayat,
        district: sensor.district,
        state: sensor.state,
        status: snapshot.status
      });
    }

    res.json({
      success: true,
      data: {
        statistics: { totalVillagers, totalSensors, totalVillages: 1, activeAlerts: 0 },
        recentVillagers,
        recentSensors
      }
    });
  } catch (error) {
    console.error('❌ Dashboard error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ==================== MOBILE AUTHENTICATION ====================

app.post('/api/login', async (req, res) => {
  const { aadhaarNumber } = req.body;
  if (!aadhaarNumber || aadhaarNumber.length !== 12) {
    return res.status(400).json({ success: false, error: 'Please enter valid 12-digit Aadhaar number' });
  }

  const isAdmin = aadhaarNumber === '999999999999';
  if (isAdmin) {
    return res.json({ success: true, token: 'token-' + Date.now(), user: { name: 'Admin User', aadhaarNumber, phone: '0000000000', role: 'admin' } });
  }

  try {
    const [rows] = await db.query(`SELECT name, phone, village, panchayat, district, state FROM villagers WHERE aadhaar = ?`, [aadhaarNumber]);
    if (rows.length === 0) return res.status(404).json({ success: false, error: 'Villager not found' });
    res.json({ success: true, token: 'villager-' + Date.now(), user: { ...rows[0], aadhaarNumber, role: 'villager' } });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/verify/check-phone', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone || phone.length !== 10) {
      return res.status(400).json({ success: false, error: 'Please enter valid 10-digit phone number' });
    }

    const villager = await fetchVillagerByPhone(phone);
    if (!villager) {
      return res.json({ success: false, error: 'Phone number not registered in our system' });
    }

    res.json({ success: true, message: 'Phone number verified', villager });
  } catch (error) {
    console.error('❌ Phone check error:', error);
    res.status(500).json({ success: false, error: 'Failed to check phone number: ' + error.message });
  }
});

app.post('/api/verify/send-otp', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone || phone.length !== 10) {
      return res.status(400).json({ success: false, error: 'Please enter valid 10-digit phone number' });
    }

    const villager = await fetchVillagerByPhone(phone);
    if (!villager) return res.status(404).json({ success: false, error: 'Phone number not registered' });

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;
    otpStore.set(phone, { otp, expiresAt, phone, villagerId: villager.id, attempts: 0 });

    console.log(`✅ OTP ${otp} generated for phone ${phone}`);
    res.json({ success: true, message: 'OTP sent successfully', otp, test_mode: true });
  } catch (error) {
    console.error('❌ OTP send error:', error);
    res.status(500).json({ success: false, error: 'Failed to send OTP: ' + error.message });
  }
});

app.post('/api/verify/check-otp', async (req, res) => {
  try {
    const { phone, otp } = req.body;
    if (!phone || !otp) return res.status(400).json({ success: false, error: 'Phone number and OTP are required' });

    const otpData = otpStore.get(phone);
    if (!otpData) return res.json({ success: false, error: 'OTP not found or expired. Please request a new one.' });
    if (Date.now() > otpData.expiresAt) {
      otpStore.delete(phone);
      return res.json({ success: false, error: 'OTP has expired. Please request a new one.' });
    }
    if (otpData.attempts >= 3) {
      otpStore.delete(phone);
      return res.json({ success: false, error: 'Too many attempts. OTP invalidated.' });
    }

    otpData.attempts++;
    if (otpData.otp !== otp) {
      otpStore.set(phone, otpData);
      return res.json({ success: false, error: 'Invalid OTP. Attempts: ' + otpData.attempts });
    }

    const villager = await fetchVillagerByPhone(phone);
    if (!villager) return res.json({ success: false, error: 'Villager data not found' });

    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count FROM sensors WHERE villager_id = ?`,
      [villager.id]
    );

    const token = 'villager-' + Date.now() + '-' + phone;
    otpStore.delete(phone);

    res.json({
      success: true,
      token,
      user: {
        name: villager.name,
        phone,
        village: villager.village,
        panchayat: villager.panchayat,
        district: villager.district,
        state: villager.state,
        role: 'villager',
        sensor_count: sensorCountResult.sensor_count || 0
      },
      permissions: { can_view: true, can_edit: false, can_delete: false }
    });
  } catch (error) {
    console.error('❌ OTP verification error:', error);
    res.status(500).json({ success: false, error: 'OTP verification failed: ' + error.message });
  }
});

app.post('/api/verify/resend-otp', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone) return res.status(400).json({ success: false, error: 'Phone number is required' });

    otpStore.delete(phone);
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;
    otpStore.set(phone, { otp, expiresAt, phone, attempts: 0 });

    console.log(`✅ New OTP ${otp} generated for ${phone}`);
    res.json({ success: true, message: 'New OTP sent successfully', otp, test_mode: true });
  } catch (error) {
    console.error('❌ Resend OTP error:', error);
    res.status(500).json({ success: false, error: 'Failed to resend OTP: ' + error.message });
  }
});

app.get('/api/auth/validate', async (req, res) => {
  const token = req.headers.authorization;
  if (!token) return res.json({ success: false, error: 'No token' });
  if (token.startsWith('villager-') || token.startsWith('token-')) {
    return res.json({ success: true, valid: true, message: 'Token is valid' });
  }
  return res.json({ success: false, valid: false, error: 'Invalid token' });
});

// ==================== DEBUG ENDPOINTS ====================

app.get('/api/debug/influx-check/:devEUI', async (req, res) => {
  try {
    const { devEUI } = req.params;
    
    const [sensorInfo] = await db.query(
      'SELECT id, name, type FROM sensors WHERE id = ?',
      [devEUI]
    );
    
    if (sensorInfo.length === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }
    
    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.devEUI == "${devEUI}")
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: 10)
    `;
    
    const data = await queryInfluxDB(fluxQuery);
    
    const availableFields = new Set();
    const sampleData = [];
    
    for (const row of data) {
      const fields = Object.keys(row).filter(k => 
        !k.startsWith('_') && k !== 'devEUI' && k !== 'result' && k !== 'table'
      );
      fields.forEach(f => availableFields.add(f));
      
      sampleData.push({
        time: row._time,
        fields: fields,
        data: row
      });
    }
    
    res.json({
      success: true,
      sensor: sensorInfo[0],
      hasData: data.length > 0,
      dataCount: data.length,
      availableFields: Array.from(availableFields),
      sampleData: sampleData.slice(0, 5),
      expectedType: sensorInfo[0].type
    });
    
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/debug/raw', async (req, res) => {
  try {
    const query = `from(bucket: "${INFLUX_CONFIG.bucket}") |> range(start: -1h) |> limit(n: 20)`;
    const result = await queryInfluxDB(query);
    res.json({ success: true, count: result.length, data: result });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ==================== UNASSIGNED SENSORS & MAPPING ====================

app.get('/api/village-sensors/unassigned', async (req, res) => {
  try {
    const { village } = req.query;
    if (!village) return res.status(400).json({ success: false, error: 'Village name is required' });

    const searchTerm = `%${String(village).trim().toLowerCase()}%`;
    
    const [sensorRows] = await db.query(
      `${SENSOR_WITH_LOCATION_SQL}
       WHERE s.villager_id IS NULL
         AND (
           LOWER(COALESCE(s.location_description, '')) LIKE ?
           OR LOWER(COALESCE(p.name, '')) LIKE ?
         )
       ORDER BY s.name ASC`,
      [searchTerm, searchTerm]
    );

    const sensors = [];
    for (const sensorRow of sensorRows) {
      sensors.push(await buildSensorPayload(sensorRow));
    }

    res.json({ success: true, village, sensors, count: sensors.length });
  } catch (err) {
    console.error('❌ Error fetching unassigned sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/sensors/map', async (req, res) => {
  try {
    const { devEUI, phone } = req.body;
    if (!devEUI || !phone) return res.status(400).json({ success: false, error: 'devEUI and phone are required' });

    const conn = await db.getConnection();
    await conn.beginTransaction();

    try {
      const villager = await fetchVillagerByPhone(phone, conn);
      if (!villager) throw new Error('Villager not found with this phone number');

      const [[sensor]] = await conn.query(
        `SELECT id, villager_id FROM sensors WHERE id = ?`,
        [devEUI]
      );
      if (!sensor) throw new Error('Sensor not found with this id');

      if (sensor.villager_id !== null) {
        throw new Error('Sensor is already mapped to another villager');
      }

      await conn.query(
        `UPDATE sensors
         SET villager_id = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [villager.id, sensor.id]
      );
      
      await conn.commit();
      res.json({ success: true, message: 'Sensor mapped successfully' });
      
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  } catch (err) {
    console.error('❌ Error mapping sensor:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== SERVING HTML PAGES ====================

app.use(express.static('public'));

app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ==================== ERROR HANDLING ====================

app.use('/api', (req, res) => {
  res.status(404).json({
    success: false,
    error: `API endpoint not found: ${req.method} ${req.originalUrl}`
  });
});

app.use((err, req, res, next) => {
  console.error('❌ Server error:', err.message);
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: err.message
  });
});

// ==================== START SERVER ====================

app.listen(PORT, () => {
  console.log('🚀 Smart Panchayat Backend');
  console.log('══════════════════════════════════════════════════════');
  console.log(`📡 Server running on port: ${PORT}`);
  console.log(`📊 InfluxDB Bucket: ${INFLUX_CONFIG.bucket}`);
  console.log(`🏢 InfluxDB Org: ${INFLUX_CONFIG.org}`);
  console.log(`🔧 API:    /api/*`);
  console.log(`📱 Mobile: /api/verify/*, /api/mobile/sensors`);
  console.log(`🏠 Admin:  http://localhost:${PORT}/admin`);
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Test endpoints:');
  console.log(`   GET /api/health - Check database connections`);
  console.log(`   GET /api/test-influx - Test InfluxDB connection`);
  console.log(`   GET /api/sensors - List all sensors`);
  console.log(`   GET /api/debug/raw - View raw InfluxDB data`);
  console.log('══════════════════════════════════════════════════════');
});
