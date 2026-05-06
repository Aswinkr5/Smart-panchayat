require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { InfluxDB } = require('@influxdata/influxdb-client');
const path = require('path');
const app = express();

// ==================== DATABASE CONFIGURATIONS ====================

// InfluxDB Configuration
const INFLUX_CONFIG = {
  url: process.env.INFLUX_URL || 'https://us-east-1-1.aws.cloud2.influxdata.com',
  token: process.env.INFLUX_TOKEN,
  org: process.env.INFLUX_ORG || 'SmartPanchayat',
  bucket: process.env.INFLUX_BUCKET || 'sensor_data'
};

const PORT = process.env.PORT || 8181;

// Initialize InfluxDB clients
const influxDB = new InfluxDB({ url: INFLUX_CONFIG.url, token: INFLUX_CONFIG.token });
const queryApi = influxDB.getQueryApi(INFLUX_CONFIG.org);

// MySQL Configuration
const mysql = require('mysql2');

console.log('🔧 Configuration:');
console.log('   InfluxDB Bucket:', INFLUX_CONFIG.bucket);
console.log('   InfluxDB Org:', INFLUX_CONFIG.org);

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
  console.log('   MySQL Host:', process.env.MYSQL_HOST);
  console.log('   MySQL Port:', process.env.MYSQL_PORT);
  db = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    port: parseInt(process.env.MYSQL_PORT) || 3306,
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
    
    console.log('✅ Database setup complete');
  } catch (error) {
    console.error('❌ MySQL connection failed:', error.message);
  }
})();

// ==================== OTP STORE ====================
const otpStore = new Map();

// Helper function to get district name from district_id
async function getDistrictName(districtId) {
  if (!districtId) return null;
  const [rows] = await db.query(
    'SELECT name FROM locations WHERE id = ? AND type = "district"',
    [districtId]
  );
  return rows.length > 0 ? rows[0].name : null;
}

// Helper function to get sensor type mapping
function getSensorTypeMapping(type) {
  const typeMap = {
    'Temp': 'temperature',
    'Temperature': 'temperature',
    'Humidity': 'humidity',
    'AirQuality': 'air_quality',
    'SoilMoisture': 'soil_moisture',
    'SoilPH': 'soil_ph',
    'WaterPH': 'water_ph',
    'WaterSalinity': 'water_salinity',
    'Rainfall': 'rainfall',
    'WaterLevel': 'water_level'
  };
  return typeMap[type] || type.toLowerCase();
}

// Updated function to fetch sensor data from InfluxDB using district_name + type
async function fetchLatestSensorSnapshot(sensorId, sensorType, districtId) {
  try {
    // Get district name
    const districtName = await getDistrictName(districtId);
    if (!districtName) {
      console.log(`⚠️ No district found for sensor ${sensorId}, district_id: ${districtId}`);
      return { measurement: 'No data', time: '', status: 'Offline', numericValue: null };
    }
    
    // Build measurement name: district_name_sensor_type
    const measurementName = `${districtName}_${sensorType}`;
    console.log(`📊 Querying InfluxDB: measurement=${measurementName}, sensor_id=${sensorId}`);
    
    const dataQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "${measurementName}")
        |> filter(fn: (r) => r.sensor_id == "${sensorId}")
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
      
      // Try to get value from different possible fields
      let value = null;
      
      // Check for value field
      if (point.value !== undefined) {
        value = point.value;
      }
      else if (point._value !== undefined) {
        value = point._value;
      }
      else {
        // Look for any numeric field
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
    } else {
      console.log(`⚠️ No data found for sensor ${sensorId} in measurement ${measurementName}`);
    }

    return { measurement, time, status, numericValue };
  } catch (error) {
    console.error(`❌ Error fetching snapshot for sensor ${sensorId}:`, error.message);
    return { measurement: 'Error', time: '', status: 'Offline', numericValue: null };
  }
}

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
    'Temp': '°C',
    'temperature': '°C',
    'Humidity': '%',
    'humidity': '%',
    'AirQuality': 'AQI',
    'air_quality': 'AQI',
    'SoilMoisture': '%',
    'soil_moisture': '%',
    'SoilPH': 'pH',
    'soil_ph': 'pH',
    'WaterPH': 'pH',
    'water_ph': 'pH',
    'WaterSalinity': 'ppm',
    'water_salinity': 'ppm',
    'Rainfall': 'mm',
    'rainfall': 'mm',
    'WaterLevel': 'm',
    'water_level': 'm'
  };
  return units[type] || '';
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

const SENSOR_ACTIVE_THRESHOLD = 22; // seconds

async function getActiveSensorCount() {
  try {
    // Get all sensors with their district info
    const [sensors] = await db.query(`
      SELECT s.id, s.type, s.district_id, l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      WHERE s.villager_id IS NOT NULL
    `);
    
    let activeCount = 0;
    
    for (const sensor of sensors) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.id, sensor.type, sensor.district_id);
      if (snapshot.status === 'Live') {
        activeCount++;
      }
    }
    
    return activeCount;
  } catch (error) {
    console.error('Error getting active sensor count:', error);
    return 0;
  }
}

function determineSensorType(name) {
    const lowerName = name.toLowerCase();
    if (lowerName.includes('temp')) return 'Temp';
    if (lowerName.includes('humidity')) return 'Humidity';
    if (lowerName.includes('air') || lowerName.includes('aqi')) return 'AirQuality';
    if (lowerName.includes('soil') && lowerName.includes('moist')) return 'SoilMoisture';
    if (lowerName.includes('soil') && lowerName.includes('ph')) return 'SoilPH';
    if (lowerName.includes('water') && lowerName.includes('ph')) return 'WaterPH';
    if (lowerName.includes('water') && lowerName.includes('salinity')) return 'WaterSalinity';
    if (lowerName.includes('rain')) return 'Rainfall';
    if (lowerName.includes('water') && lowerName.includes('level')) return 'WaterLevel';
    return 'Temp';
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

// Test InfluxDB connection with proper measurement format
app.get('/api/test-influx', async (req, res) => {
  try {
    // Test query for a known district
    const testMeasurement = 'Mala_Temp';
    const query = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "${testMeasurement}")
        |> limit(n: 1)
    `;
    const result = await queryInfluxDB(query);
    res.json({ 
      success: true, 
      message: 'InfluxDB connected', 
      bucket: INFLUX_CONFIG.bucket,
      org: INFLUX_CONFIG.org,
      testMeasurement: testMeasurement,
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
      `SELECT v.id, v.name, v.phone, v.address, v.created_at
       FROM villagers v
       ORDER BY v.created_at DESC`
    );

    res.json({ success: true, villagers: rows, count: rows.length });
  } catch (err) {
    console.error('❌ Error fetching villagers:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/villagers', async (req, res) => {
  try {
    const { name, phone, address, panchayat_id } = req.body;

    if (!name || !phone) {
      return res.status(400).json({ 
        success: false, 
        error: 'Missing required fields: name, phone' 
      });
    }

    await db.query(
      `INSERT INTO villagers (name, phone, address, panchayat_id)
       VALUES (?, ?, ?, ?)`,
      [name, phone, address, panchayat_id || null]
    );

    res.json({ success: true, message: 'Villager added successfully' });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ success: false, error: 'Phone already exists' });
    }
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== SENSOR MANAGEMENT ====================

// Get all sensors
app.get('/api/sensors', async (req, res) => {
  try {
    const [sensorRows] = await db.query(`
      SELECT s.id, s.name, s.type, s.status, s.location_description, 
             s.villager_id, s.installed_at, s.updated_at, s.district_id,
             l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      ORDER BY s.installed_at DESC
    `);

    const sensors = [];

    for (const sensor of sensorRows) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.id, sensor.type, sensor.district_id);
      
      let location = sensor.location_description || sensor.district_name || 'Unknown';
      
      sensors.push({
        devEUI: sensor.id,
        name: sensor.name || sensor.id,
        type: sensor.type,
        village: location,
        district: sensor.district_name,
        measurement: snapshot.measurement,
        time: snapshot.time,
        status: snapshot.status,
        isAssigned: sensor.villager_id !== null
      });
    }

    res.json({ success: true, sensors });
  } catch (err) {
    console.error('❌ Error fetching sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get single sensor
app.get('/api/sensors/:devEUI', async (req, res) => {
  try {
    const { devEUI } = req.params;

    const [rows] = await db.query(`
      SELECT s.id, s.name, s.type, s.status, s.location_description, 
             s.villager_id, s.district_id, l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      WHERE s.id = ?
    `, [devEUI]);

    if (rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }

    const sensor = rows[0];
    const snapshot = await fetchLatestSensorSnapshot(sensor.id, sensor.type, sensor.district_id);

    res.json({ 
      success: true, 
      sensor: {
        id: sensor.id,
        devEUI: sensor.id,
        name: sensor.name,
        type: sensor.type,
        location: sensor.location_description,
        district: sensor.district_name,
        measurement: snapshot.measurement,
        time: snapshot.time,
        status: snapshot.status,
        isAssigned: sensor.villager_id !== null
      }
    });
  } catch (err) {
    console.error('❌ Error fetching sensor:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Add new sensor
app.post('/api/sensors', async (req, res) => {
  const { devEUI, name, type, location_description, panchayat_id, district_id } = req.body;

  if (!devEUI) {
    return res.status(400).json({ success: false, error: 'devEUI is required' });
  }

  let sensorType = type || determineSensorType(name || devEUI);

  try {
    await db.query(
      `INSERT INTO sensors (id, name, type, location_description, panchayat_id, district_id, status)
       VALUES (?, ?, ?, ?, ?, ?, 'active')`,
      [devEUI, name || devEUI, sensorType, location_description || null, panchayat_id || null, district_id || null]
    );

    res.json({ success: true, message: 'Sensor registered successfully' });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ success: false, error: 'Sensor already exists' });
    }
    res.status(500).json({ success: false, error: err.message });
  }
});

// Update sensor
app.put('/api/sensors/:devEUI', async (req, res) => {
  const { devEUI } = req.params;
  const { name, type, location_description, status } = req.body;

  try {
    const [result] = await db.query(
      `UPDATE sensors SET name = ?, type = ?, location_description = ?, status = ? WHERE id = ?`,
      [name || null, type || null, location_description || null, status || 'active', devEUI]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }

    res.json({ success: true, message: 'Sensor updated successfully' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Delete sensor
app.delete('/api/sensors/:devEUI', async (req, res) => {
  const { devEUI } = req.params;

  try {
    const [result] = await db.query(`DELETE FROM sensors WHERE id = ?`, [devEUI]);

    if (result.affectedRows === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }

    res.json({ success: true, message: 'Sensor deleted successfully' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== SENSOR HISTORY ENDPOINT ====================

app.get('/api/sensors/:devEUI/history', async (req, res) => {
  try {
    const { devEUI } = req.params;
    const { range } = req.query;
    let rangeValue = range ? range : '-24h';

    console.log(`📊 Fetching history for sensor ${devEUI}, range: ${rangeValue}`);

    const [sensorInfo] = await db.query(`
      SELECT s.type, s.district_id, l.name as district_name 
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      WHERE s.id = ?
    `, [devEUI]);
    
    if (sensorInfo.length === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }
    
    const sensorType = sensorInfo[0].type;
    const districtId = sensorInfo[0].district_id;
    const districtName = sensorInfo[0].district_name;
    
    if (!districtName) {
      return res.json({ success: true, history: [], count: 0, message: 'District not found for sensor' });
    }
    
    // Build measurement name
    const measurementName = `${districtName}_${sensorType}`;
    console.log(`📊 Querying history: measurement=${measurementName}, sensor_id=${devEUI}`);

    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: ${rangeValue})
        |> filter(fn: (r) => r._measurement == "${measurementName}")
        |> filter(fn: (r) => r.sensor_id == "${devEUI}")
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
      
      // Try to get value from different possible fields
      if (row.value !== undefined) {
        value = typeof row.value === 'number' ? row.value : parseFloat(row.value);
      }
      else if (row._value !== undefined) {
        value = typeof row._value === 'number' ? row._value : parseFloat(row._value);
      }
      else {
        for (let key in row) {
          if (!key.startsWith('_') && typeof row[key] === 'number' && key !== 'sensor_id') {
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
      unit: getUnitForType(sensorType),
      measurementName: measurementName
    });

  } catch (err) {
    console.error('❌ Error fetching sensor history:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== MOBILE ENDPOINTS ====================

// Get sensors for a specific villager (mobile app)
app.get('/api/mobile/sensors', async (req, res) => {
  try {
    const { phone } = req.query;
    console.log('📱 Fetching sensors for phone:', phone);
    
    if (!phone) {
      return res.status(400).json({ success: false, error: 'Phone number is required' });
    }

    const villager = await fetchVillagerByPhone(phone);

    if (!villager) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    // Get sensors assigned to this villager with district info
    const [sensorRows] = await db.query(`
      SELECT s.id, s.name, s.type, s.location_description, s.district_id,
             l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      WHERE s.villager_id = ?
      ORDER BY s.installed_at DESC
    `, [villager.id]);

    console.log(`📊 Found ${sensorRows.length} sensors for villager ${villager.name}`);

    const sensors = [];

    for (const sensor of sensorRows) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.id, sensor.type, sensor.district_id);
      
      sensors.push({
        devEUI: sensor.id,
        name: sensor.name || sensor.id,
        type: sensor.type,
        location: sensor.location_description || sensor.district_name || 'Unknown',
        district: sensor.district_name,
        measurement: snapshot.measurement,
        time: snapshot.time,
        status: snapshot.status
      });
    }

    res.json({
      success: true,
      villager: {
        id: villager.id,
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

// ==================== UNASSIGNED SENSORS ====================

app.get('/api/village-sensors/unassigned', async (req, res) => {
  try {
    const { village } = req.query;
    console.log('📍 Fetching unassigned sensors for village:', village);
    
    if (!village) {
      return res.status(400).json({ 
        success: false, 
        error: 'Village name is required' 
      });
    }

    const searchTerm = `%${String(village).trim().toLowerCase()}%`;
    
    const [sensorRows] = await db.query(`
      SELECT s.id, s.name, s.type, s.status, s.location_description, 
             s.villager_id, s.district_id, l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      WHERE s.villager_id IS NULL
        AND (
          LOWER(COALESCE(s.location_description, '')) LIKE ?
          OR LOWER(COALESCE(s.name, '')) LIKE ?
          OR LOWER(COALESCE(l.name, '')) LIKE ?
        )
      ORDER BY s.name ASC
    `, [searchTerm, searchTerm, searchTerm]);

    console.log(`📊 Found ${sensorRows.length} unassigned sensors`);

    const sensors = [];
    
    for (const sensor of sensorRows) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.id, sensor.type, sensor.district_id);
      
      sensors.push({
        id: sensor.id,
        devEUI: sensor.id,
        name: sensor.name,
        type: sensor.type,
        location: sensor.location_description || sensor.district_name,
        district: sensor.district_name,
        measurement: snapshot.measurement,
        time: snapshot.time,
        status: snapshot.status,
        isAssigned: false
      });
    }

    res.json({ 
      success: true, 
      village: village, 
      sensors: sensors, 
      count: sensors.length 
    });
  } catch (err) {
    console.error('❌ Error fetching unassigned sensors:', err);
    res.status(500).json({ 
      success: false, 
      error: err.message 
    });
  }
});

// Map sensor to villager
app.post('/api/sensors/map', async (req, res) => {
  try {
    const { devEUI, phone } = req.body;
    console.log('📍 Mapping sensor:', devEUI, 'to phone:', phone);
    
    if (!devEUI || !phone) {
      return res.status(400).json({ 
        success: false, 
        error: 'devEUI and phone are required' 
      });
    }

    const conn = await db.getConnection();
    await conn.beginTransaction();

    try {
      const villager = await fetchVillagerByPhone(phone, conn);
      if (!villager) {
        throw new Error('Villager not found with this phone number');
      }

      const [[sensor]] = await conn.query(
        `SELECT id, villager_id FROM sensors WHERE id = ?`,
        [devEUI]
      );
      
      if (!sensor) {
        throw new Error('Sensor not found with this id');
      }

      if (sensor.villager_id !== null) {
        throw new Error('Sensor is already mapped to another villager');
      }

      await conn.query(
        `UPDATE sensors SET villager_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [villager.id, sensor.id]
      );
      
      await conn.commit();
      
      console.log('✅ Sensor mapped successfully');
      res.json({ 
        success: true, 
        message: 'Sensor mapped successfully' 
      });
      
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  } catch (err) {
    console.error('❌ Error mapping sensor:', err);
    res.status(500).json({ 
      success: false, 
      error: err.message 
    });
  }
});

// ==================== ADMIN DASHBOARD ====================

app.get('/api/admin/dashboard', async (req, res) => {
  try {
    const [[{ totalVillagers }]] = await db.query(`SELECT COUNT(*) AS totalVillagers FROM villagers`);
    const [[{ totalSensors }]] = await db.query(`SELECT COUNT(*) AS totalSensors FROM sensors`);
    const activeSensors = await getActiveSensorCount();

    const [recentVillagers] = await db.query(
      `SELECT name, phone, created_at FROM villagers ORDER BY created_at DESC LIMIT 5`
    );

    const [sensorRows] = await db.query(`
      SELECT s.id, s.name, s.type, s.status, s.district_id, l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      ORDER BY s.installed_at DESC LIMIT 5
    `);

    const recentSensors = [];
    for (const sensor of sensorRows) {
      const snapshot = await fetchLatestSensorSnapshot(sensor.id, sensor.type, sensor.district_id);
      recentSensors.push({
        devEUI: sensor.id,
        name: sensor.name || sensor.id,
        type: sensor.type,
        district: sensor.district_name,
        status: snapshot.status
      });
    }

    res.json({
      success: true,
      data: {
        statistics: { 
          totalVillagers, 
          totalSensors, 
          activeSensors,
          totalVillages: 1, 
          activeAlerts: 0 
        },
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

app.post('/api/verify/check-phone', async (req, res) => {
  try {
    const { phone } = req.body;
    console.log('📱 Checking phone number:', phone);
    
    if (!phone || phone.length !== 10) {
      return res.status(400).json({ 
        success: false, 
        error: 'Please enter valid 10-digit phone number' 
      });
    }

    const villager = await fetchVillagerByPhone(phone);
    
    if (!villager) {
      return res.json({ 
        success: false, 
        error: 'Phone number not registered in our system' 
      });
    }

    console.log('✅ Phone number verified for:', villager.name);
    res.json({ 
      success: true, 
      message: 'Phone number verified', 
      villager: {
        name: villager.name,
        phone: villager.phone,
        village: villager.village
      }
    });
  } catch (error) {
    console.error('❌ Phone check error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to check phone number: ' + error.message 
    });
  }
});

app.post('/api/verify/send-otp', async (req, res) => {
  try {
    const { phone } = req.body;
    console.log('📱 Sending OTP to:', phone);
    
    if (!phone || phone.length !== 10) {
      return res.status(400).json({ 
        success: false, 
        error: 'Please enter valid 10-digit phone number' 
      });
    }

    const villager = await fetchVillagerByPhone(phone);
    if (!villager) {
      return res.status(404).json({ 
        success: false, 
        error: 'Phone number not registered' 
      });
    }

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;
    otpStore.set(phone, { 
      otp, 
      expiresAt, 
      phone, 
      villagerId: villager.id, 
      attempts: 0 
    });

    console.log(`✅ OTP ${otp} generated for phone ${phone}`);
    
    res.json({ 
      success: true, 
      message: 'OTP sent successfully', 
      otp: otp,
      test_mode: true 
    });
  } catch (error) {
    console.error('❌ OTP send error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send OTP: ' + error.message 
    });
  }
});

app.post('/api/verify/check-otp', async (req, res) => {
  try {
    const { phone, otp } = req.body;
    console.log('🔐 Verifying OTP for:', phone);
    
    if (!phone || !otp) {
      return res.status(400).json({ 
        success: false, 
        error: 'Phone number and OTP are required' 
      });
    }

    const otpData = otpStore.get(phone);
    
    if (!otpData) {
      return res.json({ 
        success: false, 
        error: 'OTP not found or expired. Please request a new one.' 
      });
    }
    
    if (Date.now() > otpData.expiresAt) {
      otpStore.delete(phone);
      return res.json({ 
        success: false, 
        error: 'OTP has expired. Please request a new one.' 
      });
    }
    
    if (otpData.attempts >= 3) {
      otpStore.delete(phone);
      return res.json({ 
        success: false, 
        error: 'Too many attempts. OTP invalidated.' 
      });
    }

    otpData.attempts++;
    
    if (otpData.otp !== otp) {
      otpStore.set(phone, otpData);
      return res.json({ 
        success: false, 
        error: `Invalid OTP. ${3 - otpData.attempts} attempts remaining.` 
      });
    }

    const villager = await fetchVillagerByPhone(phone);
    if (!villager) {
      return res.json({ 
        success: false, 
        error: 'Villager data not found' 
      });
    }

    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count FROM sensors WHERE villager_id = ?`,
      [villager.id]
    );

    const token = 'villager-' + Date.now() + '-' + phone;
    otpStore.delete(phone);

    console.log('✅ OTP verified successfully for:', villager.name);
    
    res.json({
      success: true,
      token,
      user: {
        id: villager.id,
        name: villager.name,
        phone: villager.phone,
        village: villager.village,
        panchayat: villager.panchayat,
        district: villager.district,
        state: villager.state,
        role: 'villager',
        sensor_count: sensorCountResult.sensor_count || 0
      },
      permissions: { 
        can_view: true, 
        can_edit: false, 
        can_delete: false 
      }
    });
  } catch (error) {
    console.error('❌ OTP verification error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'OTP verification failed: ' + error.message 
    });
  }
});

app.post('/api/verify/resend-otp', async (req, res) => {
  try {
    const { phone } = req.body;
    console.log('📱 Resending OTP to:', phone);
    
    if (!phone) {
      return res.status(400).json({ 
        success: false, 
        error: 'Phone number is required' 
      });
    }

    otpStore.delete(phone);
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;
    
    otpStore.set(phone, { 
      otp, 
      expiresAt, 
      phone, 
      attempts: 0 
    });

    console.log(`✅ New OTP ${otp} generated for ${phone}`);
    
    res.json({ 
      success: true, 
      message: 'New OTP sent successfully', 
      otp: otp,
      test_mode: true 
    });
  } catch (error) {
    console.error('❌ Resend OTP error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to resend OTP: ' + error.message 
    });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { phone } = req.body;
    console.log('📱 Login attempt for phone:', phone);
    
    if (!phone || phone.length !== 10) {
      return res.status(400).json({ 
        success: false, 
        error: 'Please enter valid 10-digit phone number' 
      });
    }

    const villager = await fetchVillagerByPhone(phone);
    
    if (!villager) {
      return res.status(404).json({ 
        success: false, 
        error: 'Villager not found' 
      });
    }
    
    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count FROM sensors WHERE villager_id = ?`,
      [villager.id]
    );
    
    const token = 'villager-' + Date.now() + '-' + phone;
    
    res.json({ 
      success: true, 
      token: token, 
      user: {
        id: villager.id,
        name: villager.name,
        phone: villager.phone,
        village: villager.village,
        panchayat: villager.panchayat,
        district: villager.district,
        state: villager.state,
        role: 'villager',
        sensor_count: sensorCountResult.sensor_count || 0
      }
    });
  } catch (err) {
    console.error('❌ Login error:', err);
    res.status(500).json({ 
      success: false, 
      error: err.message 
    });
  }
});

app.get('/api/auth/validate', async (req, res) => {
  const token = req.headers.authorization;
  if (!token) {
    return res.json({ success: false, error: 'No token' });
  }
  if (token.startsWith('villager-') || token.startsWith('token-')) {
    return res.json({ success: true, valid: true, message: 'Token is valid' });
  }
  return res.json({ success: false, valid: false, error: 'Invalid token' });
});

// ==================== DEBUG ENDPOINTS ====================

app.get('/api/debug/influx-check/:devEUI', async (req, res) => {
  try {
    const { devEUI } = req.params;
    
    const [sensorInfo] = await db.query(`
      SELECT s.id, s.name, s.type, s.district_id, l.name as district_name
      FROM sensors s
      LEFT JOIN locations l ON l.id = s.district_id
      WHERE s.id = ?
    `, [devEUI]);
    
    if (sensorInfo.length === 0) {
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }
    
    const sensor = sensorInfo[0];
    const measurementName = `${sensor.district_name}_${sensor.type}`;
    
    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "${measurementName}")
        |> filter(fn: (r) => r.sensor_id == "${devEUI}")
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: 10)
    `;
    
    const data = await queryInfluxDB(fluxQuery);
    
    const availableFields = new Set();
    const sampleData = [];
    
    for (const row of data) {
      const fields = Object.keys(row).filter(k => 
        !k.startsWith('_') && k !== 'sensor_id' && k !== 'result' && k !== 'table'
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
      sensor: {
        id: sensor.id,
        name: sensor.name,
        type: sensor.type,
        district: sensor.district_name,
        district_id: sensor.district_id
      },
      measurementName: measurementName,
      hasData: data.length > 0,
      dataCount: data.length,
      availableFields: Array.from(availableFields),
      sampleData: sampleData.slice(0, 5),
      expectedMeasurement: measurementName
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
  console.log(`📋 Unassigned Sensors: /api/village-sensors/unassigned?village=NAME`);
  console.log(`🔗 Map Sensor: /api/sensors/map`);
  console.log(`🏠 Admin:  http://localhost:${PORT}/admin`);
  console.log('══════════════════════════════════════════════════════');
  console.log('📊 InfluxDB Query Format:');
  console.log('   Measurement = <district_name>_<sensor_type>');
  console.log('   Example: Mala_Temp, Thrissur_Humidity');
  console.log('   Tag: sensor_id = <sensor_id>');
  console.log('══════════════════════════════════════════════════════');
});
