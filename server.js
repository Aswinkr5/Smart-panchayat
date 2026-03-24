require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const path = require('path');
const app = express();

// ==================== DATABASE CONFIGURATIONS ====================

// InfluxDB Configuration
const INFLUX_CONFIG = {
  url: process.env.INFLUX_URL || 'https://us-east-1-1.aws.cloud2.influxdata.com',
  token: process.env.INFLUX_TOKEN,
  org: process.env.INFLUX_ORG || 'Smart Panchayat',
  bucket: process.env.INFLUX_BUCKET || 'smart_panchayat'
};

const PORT = process.env.PORT || 8181;

// Initialize InfluxDB clients
const influxDB = new InfluxDB({ url: INFLUX_CONFIG.url, token: INFLUX_CONFIG.token });
const writeApi = influxDB.getWriteApi(INFLUX_CONFIG.org, INFLUX_CONFIG.bucket);
const queryApi = influxDB.getQueryApi(INFLUX_CONFIG.org);

// MySQL Configuration - Fixed for Render/Railway connection
const mysql = require('mysql2');

console.log('🔧 MySQL Configuration:');

let db;
// Check if MYSQL_URL is provided (from Railway)
if (process.env.MYSQL_URL) {
  console.log('   Using MYSQL_URL connection');
  db = mysql.createPool({
    uri: process.env.MYSQL_URL,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  }).promise();
} else {
  // Use individual variables
  console.log('   Host:', process.env.MYSQL_HOST);
  console.log('   Database:', process.env.MYSQL_DATABASE);
  console.log('   Port:', process.env.MYSQL_PORT);
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
    
    // Create system_settings table if not exists
    await db.query(`
      CREATE TABLE IF NOT EXISTS system_settings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        setting_key VARCHAR(100) UNIQUE NOT NULL,
        setting_value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    
    // Insert mock data setting if not exists
    await db.query(`
      INSERT INTO system_settings (setting_key, setting_value) 
      VALUES ('mock_data_enabled', 'false')
      ON DUPLICATE KEY UPDATE setting_key = setting_key
    `);
    
    // Add type column to sensors if not exists
    try {
      await db.query(`
        ALTER TABLE sensors 
        ADD COLUMN IF NOT EXISTS type VARCHAR(50) DEFAULT 'general' AFTER name
      `);
      console.log('✅ Sensors table updated with type column');
    } catch (error) {
      console.log('ℹ️ Type column may already exist');
    }
    
    console.log('✅ System settings table ready');
  } catch (error) {
    console.error('❌ MySQL connection failed:', error.message);
  }
})();

// ==================== OTP STORE ====================
const otpStore = new Map();

// ==================== MOCK DATA GENERATOR ====================

// Configuration for mock data
const MOCK_CONFIG = {
  enabled: false,
  interval: 7000, // 7 seconds (changed from 15000)
  timer: null
};

// Define realistic value ranges for different sensor types
function getRandomValueForType(type, sensorName) {
  const name = sensorName.toLowerCase();
  
  switch(type) {
      case 'temperature':
          // 20-35°C with one decimal
          return (20 + Math.random() * 15).toFixed(1);
      case 'humidity':
          // 40-90% with no decimal (integer)
          return Math.floor(40 + Math.random() * 50).toString();
      case 'pressure':
          // 980-1020 hPa (integer)
          return Math.floor(980 + Math.random() * 40).toString();
      case 'water_level':
          // 30-100% with one decimal
          return (30 + Math.random() * 70).toFixed(1);
      case 'water_quality':
          // 6.5-8.5 pH with one decimal
          return (6.5 + Math.random() * 2).toFixed(1);
      case 'air_quality':
          // 50-200 AQI (integer)
          return Math.floor(50 + Math.random() * 150).toString();
      case 'ph':
          // 6.5-8.5 pH with one decimal
          return (6.5 + Math.random() * 2).toFixed(1);
      case 'gas':
          // 10-100 ppm (integer)
          return Math.floor(10 + Math.random() * 90).toString();
      case 'turbidity':
          // 1-100 NTU with one decimal
          return (1 + Math.random() * 99).toFixed(1);
      case 'chlorine':
          // 0.5-2.0 mg/L with two decimals
          return (0.5 + Math.random() * 1.5).toFixed(2);
      case 'air':
          // 20-35°C for air temperature
          return (20 + Math.random() * 15).toFixed(1);
      case 'water':
          // 15-30°C for water temperature
          return (15 + Math.random() * 15).toFixed(1);
      default:
          // Fallback based on sensor name
          if (name.includes('temperature')) return (20 + Math.random() * 15).toFixed(1);
          if (name.includes('humidity')) return Math.floor(40 + Math.random() * 50).toString();
          if (name.includes('water') && name.includes('level')) return (30 + Math.random() * 70).toFixed(1);
          if (name.includes('water') && name.includes('quality')) return (6.5 + Math.random() * 2).toFixed(1);
          if (name.includes('pressure')) return Math.floor(980 + Math.random() * 40).toString();
          if (name.includes('air') && name.includes('quality')) return Math.floor(50 + Math.random() * 150).toString();
          if (name.includes('ph')) return (6.5 + Math.random() * 2).toFixed(1);
          // Default random value between 10-100 with one decimal
          return (10 + Math.random() * 90).toFixed(1);
  }
}

// Get unit based on sensor type
function getUnitForType(type, sensorName) {
  const name = sensorName.toLowerCase();
  
  switch(type) {
      case 'temperature': return '°C';
      case 'humidity': return '%';
      case 'pressure': return 'hPa';
      case 'water_level': return '%';
      case 'water_quality': return 'pH';
      case 'air_quality': return 'AQI';
      case 'ph': return 'pH';
      case 'gas': return 'ppm';
      case 'turbidity': return 'NTU';
      case 'chlorine': return 'mg/L';
      case 'air': return '°C';
      case 'water': return '°C';
      default:
          if (name.includes('temperature')) return '°C';
          if (name.includes('humidity')) return '%';
          if (name.includes('water') && name.includes('level')) return '%';
          if (name.includes('water') && name.includes('quality')) return 'pH';
          if (name.includes('pressure')) return 'hPa';
          if (name.includes('air') && name.includes('quality')) return 'AQI';
          if (name.includes('ph')) return 'pH';
          return '';
  }
}

// Get field name for InfluxDB
function getFieldName(sensorName, unit) {
  // Clean the sensor name to create a proper field name
  let fieldName = sensorName.toLowerCase()
      .replace(/[^a-z0-9]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '');
  
  if (unit) {
      fieldName = `${fieldName}_${unit.replace(/[^a-z0-9]/gi, '')}`;
  }
  
  return fieldName;
}

// Generate and write mock data for all sensors
async function generateMockData() {
  try {
      // Get all sensors with their types
      const [sensors] = await db.query(
          'SELECT devEUI, name, type FROM sensors ORDER BY id DESC'
      );
      
      if (sensors.length === 0) {
          console.log('⚠️ No sensors found to generate mock data');
          return;
      }
      
      console.log(`🎲 Generating mock data for ${sensors.length} sensors...`);
      let generatedCount = 0;
      
      for (const sensor of sensors) {
          const value = getRandomValueForType(sensor.type, sensor.name);
          const unit = getUnitForType(sensor.type, sensor.name);
          const fieldName = getFieldName(sensor.name, unit);
          
          // Write to InfluxDB
          await writeToInfluxDB(
              'sensor_data',
              { devEUI: sensor.devEUI },
              { _field: fieldName, _value: value.toString() }
          );
          
          console.log(`   ✓ ${sensor.name} (${sensor.type}): ${value} ${unit}`);
          generatedCount++;
      }
      
      console.log(`✅ Mock data generated successfully for ${generatedCount} sensors at ${new Date().toLocaleTimeString()}`);
      
  } catch (error) {
      console.error('❌ Mock data generation error:', error);
  }
}

// Start/Stop mock data generator
async function startMockDataGenerator() {
  // Check if mock data is enabled in database
  try {
      const [rows] = await db.query(
          "SELECT setting_value FROM system_settings WHERE setting_key = 'mock_data_enabled'"
      );
      
      if (rows.length > 0) {
          MOCK_CONFIG.enabled = rows[0].setting_value === 'true';
      }
      
      if (MOCK_CONFIG.enabled && !MOCK_CONFIG.timer) {
          console.log('🎲 Starting mock data generator (every 7 seconds)...');
          // Generate immediately
          await generateMockData();
          // Then set interval (7 seconds)
          MOCK_CONFIG.timer = setInterval(generateMockData, MOCK_CONFIG.interval);
      } else if (!MOCK_CONFIG.enabled && MOCK_CONFIG.timer) {
          console.log('🛑 Stopping mock data generator...');
          clearInterval(MOCK_CONFIG.timer);
          MOCK_CONFIG.timer = null;
      }
  } catch (error) {
      console.error('❌ Error checking mock data status:', error);
  }
}
// Toggle mock data (called from API)
async function toggleMockData(enabled) {
    try {
        await db.query(
            "UPDATE system_settings SET setting_value = ? WHERE setting_key = 'mock_data_enabled'",
            [enabled ? 'true' : 'false']
        );
        MOCK_CONFIG.enabled = enabled;
        await startMockDataGenerator();
        return { success: true, enabled: MOCK_CONFIG.enabled };
    } catch (error) {
        console.error('❌ Error toggling mock data:', error);
        return { success: false, error: error.message };
    }
}

// Get mock data status
async function getMockDataStatus() {
    try {
        const [rows] = await db.query(
            "SELECT setting_value FROM system_settings WHERE setting_key = 'mock_data_enabled'"
        );
        return {
            enabled: rows.length > 0 ? rows[0].setting_value === 'true' : false,
            interval: MOCK_CONFIG.interval / 1000
        };
    } catch (error) {
        return { enabled: false, interval: 15 };
    }
}

// Start the generator after database connection
setTimeout(() => {
    startMockDataGenerator();
}, 5000);

// ==================== MIDDLEWARE ====================

app.use(cors({
  origin: '*',
  credentials: false,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept']
}));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');

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

// Query InfluxDB helper
async function queryInfluxDB(fluxQuery) {
  try {
    const result = await queryApi.collectRows(fluxQuery);
    return result || [];
  } catch (error) {
    console.error('❌ InfluxDB query error:', error.message);
    return [];
  }
}

// Write to InfluxDB helper
async function writeToInfluxDB(measurement, tags, fields) {
  try {
    const point = new Point(measurement);

    Object.entries(tags).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        point.tag(key, value.toString());
      }
    });

    Object.entries(fields).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        point.stringField(key, value.toString());
      }
    });

    writeApi.writePoint(point);
    await writeApi.flush();
    console.log(`✅ Written to InfluxDB: ${measurement}`);
    return true;
  } catch (error) {
    console.error('❌ InfluxDB write error:', error.message);
    return false;
  }
}

// Get active sensor count from InfluxDB
const SENSOR_ACTIVE_THRESHOLD = 20; // seconds

async function getActiveSensorCount() {
  const query = `
    from(bucket: "${INFLUX_CONFIG.bucket}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> group(columns: ["devEUI"])
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1)
      |> keep(columns: ["devEUI", "_time"])
  `;

  const rows = await queryInfluxDB(query);
  const now = Date.now();

  return rows.filter(r => {
    const diffSeconds = (now - new Date(r._time).getTime()) / 1000;
    return diffSeconds <= SENSOR_ACTIVE_THRESHOLD;
  }).length;
}

// Get all active sensors from InfluxDB
async function getActiveSensors() {
  const query = `
    from(bucket: "${INFLUX_CONFIG.bucket}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> keep(columns: ["devEUI"])
      |> group(columns: ["devEUI"])
  `;

  const rows = await queryInfluxDB(query);
  const unique = new Set(rows.map(r => r.devEUI));
  return Array.from(unique);
}

// Determine sensor type from name
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

// Test endpoint
app.get('/api/test', (req, res) => {
  res.json({
    success: true,
    message: 'API is working!',
    timestamp: new Date().toISOString()
  });
});

// Health check
app.get('/api/health', async (req, res) => {
  try {
    // Test MySQL connection
    await db.query('SELECT 1');
    
    res.json({
      success: true,
      message: 'Smart Panchayat Backend is running',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      databases: {
        mysql: 'connected',
        influxdb: 'connected'
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

// ==================== MOCK DATA CONTROL ENDPOINTS ====================

// Get mock data status
app.get('/api/admin/mock-status', async (req, res) => {
    try {
        const status = await getMockDataStatus();
        res.json({
            success: true,
            ...status
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Enable mock data
app.post('/api/admin/mock-enable', async (req, res) => {
    try {
        const result = await toggleMockData(true);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Disable mock data
app.post('/api/admin/mock-disable', async (req, res) => {
    try {
        const result = await toggleMockData(false);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Generate mock data once (manual trigger)
app.post('/api/admin/mock-generate', async (req, res) => {
    try {
        await generateMockData();
        res.json({ success: true, message: 'Mock data generated' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ==================== VILLAGER MANAGEMENT (MySQL) ====================

// Get all villagers
app.get('/api/villagers', async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT
         id,
         aadhaar AS aadhaar_number,
         name,
         phone,
         village,
         panchayat,
         district,
         state,
         occupation,
         address,
         created_at
       FROM villagers
       ORDER BY created_at DESC`
    );

    res.json({
      success: true,
      villagers: rows,
      count: rows.length
    });
  } catch (err) {
    console.error('❌ Error fetching villagers:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get a specific villager
app.get('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;

    const [rows] = await db.query(
      `SELECT
         id,
         aadhaar AS aadhaar_number,
         name,
         phone,
         village,
         panchayat,
         district,
         state,
         occupation,
         address,
         created_at
       FROM villagers
       WHERE aadhaar = ?`,
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

// Add new villager
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

// Update a villager
app.put('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    const { name, phone, village, panchayat, district, state, occupation, address } = req.body;

    const [result] = await db.query(
      `UPDATE villagers
       SET name = ?, phone = ?, village = ?, panchayat = ?, district = ?, state = ?, occupation = ?, address = ?
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

// Delete a villager
app.delete('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;

    const [result] = await db.query(
      `DELETE FROM villagers WHERE aadhaar = ?`,
      [aadhaarNumber]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    res.json({ success: true, message: 'Villager deleted' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get sensors for a specific villager
app.get('/api/villagers/:aadhaar/sensors', async (req, res) => {
  try {
    const { aadhaar } = req.params;

    // Get villager
    const [[villager]] = await db.query(
      `SELECT id, name, aadhaar, phone, village, panchayat, district, state
       FROM villagers WHERE aadhaar = ?`,
      [aadhaar]
    );

    if (!villager) {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    // Get mapped sensors
    const [sensors] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s
       JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE vs.villager_id = ?`,
      [villager.id]
    );

    const result = [];

    // Fetch latest measurement from InfluxDB for each sensor
    for (const sensor of sensors) {
      const flux = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
      `;

      const data = await queryInfluxDB(flux);

      let measurement = 'No data';
      let time = '';
      let status = 'Offline';

      if (data.length > 0) {
        measurement = `${data[0]._field}: ${data[0]._value}`;
        const t = new Date(data[0]._time);
        time = t.toLocaleString();
        status = (Date.now() - t.getTime()) / 1000 <= 22 ? 'Live' : 'Offline';
      }

      result.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        type: sensor.type,
        village: sensor.village,
        panchayat: sensor.panchayat,
        district: sensor.district,
        state: sensor.state,
        measurement,
        time,
        status
      });
    }

    res.json({
      success: true,
      villager,
      sensors: result
    });
  } catch (err) {
    console.error('❌ Error fetching villager sensors:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== SENSOR MANAGEMENT ====================

// Get all sensors (ADMIN ONLY - shows all sensors)
app.get('/api/sensors', async (req, res) => {
  try {
    // Get sensor metadata from MySQL
    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, type, village, panchayat, district, state
       FROM sensors
       ORDER BY id DESC`
    );

    const sensors = [];

    // For each sensor, get latest measurement from InfluxDB
    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district, state } = sensor;

      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${devEUI}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
      `;

      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';

      if (data.length > 0) {
        latestValue = `${data[0]._field}: ${data[0]._value}`;
        const t = new Date(data[0]._time);
        latestTime = t.toLocaleString();
        const now = new Date();
        const diffSeconds = (now - t) / 1000;
        status = diffSeconds <= 22 ? 'Live' : 'Offline';
      }

      sensors.push({
        devEUI,
        name,
        type,
        village,
        panchayat,
        district,
        state,
        measurement: latestValue,
        time: latestTime,
        status
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

    const [rows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state,
              v.phone, v.name AS villager_name, v.aadhaar
       FROM sensors s
       LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
       LEFT JOIN villagers v ON v.id = vs.villager_id
       WHERE s.devEUI = ?`,
      [devEUI]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Sensor not found'
      });
    }

    res.json({
      success: true,
      sensor: rows[0]
    });
  } catch (err) {
    console.error('❌ Error fetching sensor:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Add new sensor
app.post('/api/sensors', async (req, res) => {
  const { devEUI, deviceName, type, village, panchayat, district, state, phone } = req.body;

  if (!devEUI || !deviceName) {
    return res.status(400).json({
      success: false,
      error: 'devEUI and deviceName are required'
    });
  }

  // Auto-determine type if not provided
  let sensorType = type || determineSensorType(deviceName);

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    // Insert sensor with type, district and state
    const [sensorResult] = await conn.query(
      `INSERT INTO sensors (devEUI, name, type, village, panchayat, district, state)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [devEUI, deviceName, sensorType, village || null, panchayat || null, district || null, state || null]
    );

    const sensorId = sensorResult.insertId;

    // Map sensor to villager if phone provided
    if (phone) {
      const [[villager]] = await conn.query(
        `SELECT id FROM villagers WHERE phone = ?`,
        [phone]
      );

      if (!villager) {
        throw new Error('No villager found with this phone number');
      }

      await conn.query(
        `INSERT INTO villager_sensors (villager_id, sensor_id, phone)
         VALUES (?, ?, ?)`,
        [villager.id, sensorId, phone]
      );
    }

    await conn.commit();

    res.json({
      success: true,
      message: phone
        ? 'Sensor registered and mapped to villager'
        : 'Sensor registered successfully',
      sensor: {
        devEUI,
        name: deviceName,
        type: sensorType
      }
    });
  } catch (err) {
    await conn.rollback();

    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({
        success: false,
        error: 'Sensor already exists'
      });
    }

    res.status(400).json({
      success: false,
      error: err.message
    });
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

    // Update sensor metadata
    const [result] = await conn.query(
      `UPDATE sensors
       SET name = ?, type = ?, village = ?, panchayat = ?, district = ?, state = ?
       WHERE devEUI = ?`,
      [deviceName, type || 'general', village || null, panchayat || null, district || null, state || null, devEUI]
    );

    if (result.affectedRows === 0) {
      throw new Error('Sensor not found');
    }

    // Update mapping
    await conn.query(
      `DELETE FROM villager_sensors WHERE sensor_id =
       (SELECT id FROM sensors WHERE devEUI = ?)`,
      [devEUI]
    );

    if (phone) {
      const [[villager]] = await conn.query(
        `SELECT id FROM villagers WHERE phone = ?`,
        [phone]
      );

      if (!villager) {
        throw new Error('Villager not found');
      }

      await conn.query(
        `INSERT INTO villager_sensors (villager_id, sensor_id, phone)
         VALUES (?, (SELECT id FROM sensors WHERE devEUI = ?), ?)`,
        [villager.id, devEUI, phone]
      );
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

  if (!devEUI) {
    return res.status(400).json({
      success: false,
      error: 'devEUI is required'
    });
  }

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    // Get sensor id
    const [[sensor]] = await conn.query(
      `SELECT id FROM sensors WHERE devEUI = ?`,
      [devEUI]
    );

    if (!sensor) {
      return res.status(404).json({
        success: false,
        error: 'Sensor not found'
      });
    }

    // Remove mappings
    await conn.query(
      `DELETE FROM villager_sensors WHERE sensor_id = ?`,
      [sensor.id]
    );

    // Delete sensor
    await conn.query(
      `DELETE FROM sensors WHERE id = ?`,
      [sensor.id]
    );

    await conn.commit();

    res.json({
      success: true,
      message: 'Sensor deleted successfully'
    });
  } catch (err) {
    await conn.rollback();
    res.status(500).json({
      success: false,
      error: err.message
    });
  } finally {
    conn.release();
  }
});

// ==================== DISTRICT-BASED SENSOR QUERY ====================

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

    // Get sensors from MySQL by district
    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, type, village, panchayat, district, state
       FROM sensors 
       WHERE district = ? 
       ORDER BY name ASC`,
      [district]
    );

    console.log(`📊 Found ${sensorRows.length} sensors in ${district} district`);

    const sensors = [];

    // For each sensor, get latest measurement from InfluxDB
    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district: sensorDistrict, state } = sensor;

      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${devEUI}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
      `;

      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';
      let isAssigned = false;

      // Check if sensor is already assigned to any villager
      const [[assignment]] = await db.query(
        `SELECT id FROM villager_sensors WHERE sensor_id = ?`,
        [sensor.id]
      );
      isAssigned = assignment !== null;

      if (data.length > 0) {
        latestValue = `${data[0]._field}: ${data[0]._value}`;
        const t = new Date(data[0]._time);
        latestTime = t.toLocaleString();
        const now = new Date();
        const diffSeconds = (now - t) / 1000;
        status = diffSeconds <= 22 ? 'Live' : 'Offline';
      }

      sensors.push({
        devEUI,
        name,
        type,
        village: village || 'Unknown',
        panchayat: panchayat || 'Unknown',
        district: sensorDistrict,
        state,
        measurement: latestValue,
        time: latestTime,
        status,
        isAssigned
      });
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

// Get list of all districts that have sensors
app.get('/api/districts', async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT DISTINCT district FROM sensors WHERE district IS NOT NULL AND district != '' ORDER BY district`
    );

    const districts = rows.map(row => row.district);

    res.json({
      success: true,
      districts: districts,
      count: districts.length
    });

  } catch (err) {
    console.error('❌ Error fetching districts:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== MOBILE ENDPOINTS ====================

// Get sensors for logged-in user (MOBILE ONLY - shows only user's sensors)
app.get('/api/mobile/sensors', async (req, res) => {
  try {
    const { phone } = req.query;

    console.log('📱 MOBILE SENSORS request for phone:', phone);

    if (!phone) {
      return res.status(400).json({
        success: false,
        error: 'Phone number is required'
      });
    }

    // Get villager ID
    const [[villager]] = await db.query(
      `SELECT id, name, phone, village, panchayat, district, state
       FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (!villager) {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    // Get only sensors mapped to this villager
    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s
       JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE vs.villager_id = ?
       ORDER BY s.id DESC`,
      [villager.id]
    );

    const sensors = [];

    // For each sensor, get latest measurement from InfluxDB
    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district, state } = sensor;

      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${devEUI}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
      `;

      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';

      if (data.length > 0) {
        latestValue = `${data[0]._field}: ${data[0]._value}`;
        const t = new Date(data[0]._time);
        latestTime = t.toLocaleString();
        const now = new Date();
        const diffSeconds = (now - t) / 1000;
        status = diffSeconds <= 22 ? 'Live' : 'Offline';
      }

      sensors.push({
        devEUI,
        name,
        type,
        village,
        panchayat,
        district,
        state,
        measurement: latestValue,
        time: latestTime,
        status
      });
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
      sensors: sensors,
      sensorCount: sensors.length
    });
  } catch (err) {
    console.error('❌ Error fetching mobile sensors:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== ADMIN DASHBOARD ====================

app.get('/api/admin/dashboard', async (req, res) => {
  try {
    // Get statistics from MySQL
    const [[{ totalVillagers }]] = await db.query(
      `SELECT COUNT(*) AS totalVillagers FROM villagers`
    );

    // Get active sensor count from InfluxDB
    const totalSensors = await getActiveSensorCount();

    // Get recent villagers
    const [recentVillagers] = await db.query(
      `SELECT
         name,
         aadhaar AS aadhaar_number,
         village,
         panchayat,
         district,
         state,
         phone
       FROM villagers
       ORDER BY created_at DESC
       LIMIT 5`
    );

    // Get recent sensors with status
    const [sensorRows] = await db.query(
      `SELECT devEUI, name, type, village, panchayat, district, state 
       FROM sensors 
       ORDER BY installed_at DESC 
       LIMIT 5`
    );

    const recentSensors = [];
    for (const sensor of sensorRows) {
      const flux = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
      `;

      const data = await queryInfluxDB(flux);
      let status = 'Offline';
      
      if (data.length > 0) {
        const t = new Date(data[0]._time);
        status = (Date.now() - t.getTime()) / 1000 <= 22 ? 'Live' : 'Offline';
      }

      recentSensors.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        type: sensor.type,
        village: sensor.village,
        panchayat: sensor.panchayat,
        district: sensor.district,
        state: sensor.state,
        status
      });
    }

    res.json({
      success: true,
      data: {
        statistics: {
          totalVillagers,
          totalSensors,
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

// Aadhaar login
app.post('/api/login', async (req, res) => {
  const { aadhaarNumber } = req.body;

  if (!aadhaarNumber || aadhaarNumber.length !== 12) {
    return res.status(400).json({
      success: false,
      error: 'Please enter valid 12-digit Aadhaar number'
    });
  }

  const isAdmin = aadhaarNumber === '999999999999';

  if (isAdmin) {
    return res.json({
      success: true,
      token: 'token-' + Date.now(),
      user: {
        name: 'Admin User',
        aadhaarNumber: aadhaarNumber,
        phone: '0000000000',
        role: 'admin'
      }
    });
  }

  // Check if villager exists
  try {
    const [rows] = await db.query(
      `SELECT name, phone, village, panchayat, district, state
       FROM villagers WHERE aadhaar = ?`,
      [aadhaarNumber]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    res.json({
      success: true,
      token: 'villager-' + Date.now(),
      user: {
        name: rows[0].name,
        aadhaarNumber: aadhaarNumber,
        phone: rows[0].phone,
        village: rows[0].village,
        panchayat: rows[0].panchayat,
        district: rows[0].district,
        state: rows[0].state,
        role: 'villager'
      }
    });
  } catch (err) {
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Check phone number exists
app.post('/api/verify/check-phone', async (req, res) => {
  try {
    const { phone } = req.body;

    console.log('📱 CHECK PHONE request:', { phone });

    if (!phone || phone.length !== 10) {
      return res.status(400).json({
        success: false,
        error: 'Please enter valid 10-digit phone number'
      });
    }

    // Query MySQL for phone number
    const [rows] = await db.query(
      `SELECT id, aadhaar, name, village, panchayat, district, state
       FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (rows.length === 0) {
      console.log('❌ Phone not found in database');
      return res.json({
        success: false,
        error: 'Phone number not registered in our system'
      });
    }

    const villager = rows[0];
    console.log(`✅ Phone verified: ${phone} belongs to ${villager.name}`);

    res.json({
      success: true,
      message: 'Phone number verified',
      villager: {
        id: villager.id,
        aadhaar_number: villager.aadhaar,
        name: villager.name,
        phone: phone,
        village: villager.village,
        panchayat: villager.panchayat,
        district: villager.district,
        state: villager.state,
        role: 'villager'
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

// Send OTP
app.post('/api/verify/send-otp', async (req, res) => {
  try {
    const { phone } = req.body;

    console.log('📱 SEND OTP request:', { phone });

    if (!phone || phone.length !== 10) {
      return res.status(400).json({
        success: false,
        error: 'Please enter valid 10-digit phone number'
      });
    }

    // First check if phone exists
    const [rows] = await db.query(
      `SELECT aadhaar FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Phone number not registered'
      });
    }

    const aadhaarNumber = rows[0].aadhaar;

    // Generate 6-digit OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000; // 5 minutes

    // Store OTP with phone as key
    otpStore.set(phone, {
      otp,
      expiresAt,
      phone,
      aadhaarNumber,
      attempts: 0
    });

    console.log(`✅ OTP ${otp} generated for phone ${phone}`);

    res.json({
      success: true,
      message: 'OTP sent successfully',
      otp: otp, // Remove this in production
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

// Verify OTP
app.post('/api/verify/check-otp', async (req, res) => {
  try {
    const { phone, otp } = req.body;

    console.log('🔍 VERIFY OTP request:', { phone });

    if (!phone || !otp) {
      return res.status(400).json({
        success: false,
        error: 'Phone number and OTP are required'
      });
    }

    const otpData = otpStore.get(phone);

    // Check if OTP exists
    if (!otpData) {
      return res.json({
        success: false,
        error: 'OTP not found or expired. Please request a new one.'
      });
    }

    // Check if expired
    if (Date.now() > otpData.expiresAt) {
      otpStore.delete(phone);
      return res.json({
        success: false,
        error: 'OTP has expired. Please request a new one.'
      });
    }

    // Check attempts
    if (otpData.attempts >= 3) {
      otpStore.delete(phone);
      return res.json({
        success: false,
        error: 'Too many attempts. OTP invalidated.'
      });
    }

    // Verify OTP
    otpData.attempts++;
    if (otpData.otp !== otp) {
      otpStore.set(phone, otpData);
      return res.json({
        success: false,
        error: 'Invalid OTP. Attempts: ' + otpData.attempts
      });
    }

    // OTP is correct - get villager data
    const [rows] = await db.query(
      `SELECT aadhaar, name, village, panchayat, district, state
       FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (rows.length === 0) {
      return res.json({
        success: false,
        error: 'Villager data not found'
      });
    }

    const villager = rows[0];

    // Get sensor count for this villager
    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count 
       FROM villager_sensors vs
       JOIN villagers v ON v.id = vs.villager_id
       WHERE v.phone = ?`,
      [phone]
    );

    const sensorCount = sensorCountResult.sensor_count || 0;

    // Generate token
    const token = 'villager-' + Date.now() + '-' + phone;

    // Clear OTP after successful verification
    otpStore.delete(phone);

    console.log(`✅ OTP verified for phone ${phone}, ${sensorCount} sensors found`);

    res.json({
      success: true,
      token: token,
      user: {
        name: villager.name,
        aadhaar_number: villager.aadhaar,
        phone: phone,
        village: villager.village,
        panchayat: villager.panchayat,
        district: villager.district,
        state: villager.state,
        role: 'villager',
        sensor_count: sensorCount
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

// Resend OTP
app.post('/api/verify/resend-otp', async (req, res) => {
  try {
    const { phone } = req.body;

    console.log('🔄 RESEND OTP request:', { phone });

    if (!phone) {
      return res.status(400).json({
        success: false,
        error: 'Phone number is required'
      });
    }

    // Clear any existing OTP
    otpStore.delete(phone);

    // Generate new OTP
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

// Validate token
app.get('/api/auth/validate', async (req, res) => {
  const token = req.headers.authorization;
  if (!token) {
    return res.json({ success: false, error: 'No token' });
  }

  // Simple token validation
  if (token.startsWith('villager-') || token.startsWith('token-')) {
    return res.json({
      success: true,
      valid: true,
      message: 'Token is valid'
    });
  }

  return res.json({
    success: false,
    valid: false,
    error: 'Invalid token'
  });
});

// ==================== DEBUG ENDPOINTS ====================

// Debug raw InfluxDB data
app.get('/api/debug/raw', async (req, res) => {
  try {
    const query = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => true)
        |> limit(n: 50)
    `;

    const result = await queryInfluxDB(query);

    res.json({
      success: true,
      data: result,
      count: result.length,
      message: 'All raw data from InfluxDB'
    });
  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// Debug MySQL tables
app.get('/api/debug/mysql-tables', async (req, res) => {
  try {
    const [tables] = await db.query('SHOW TABLES');
    res.json({
      success: true,
      tables: tables
    });
  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// Debug MySQL table structure
app.get('/api/debug/table/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const [columns] = await db.query(`DESCRIBE ${tableName}`);
    res.json({
      success: true,
      table: tableName,
      columns: columns
    });
  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// ==================== UNASSIGNED SENSORS & MAPPING ====================

// Get unassigned sensors by village
app.get('/api/village-sensors/unassigned', async (req, res) => {
  try {
    const { village } = req.query;
    
    console.log('📍 VILLAGE-SENSORS: Searching for unassigned sensors in village:', village);

    if (!village) {
      return res.status(400).json({
        success: false,
        error: 'Village name is required'
      });
    }

    // First check if the village exists
    const [villageCheck] = await db.query(
      `SELECT DISTINCT village FROM sensors WHERE village = ?`,
      [village]
    );

    if (villageCheck.length === 0) {
      return res.json({
        success: true,
        village: village,
        sensors: [],
        count: 0,
        message: `No sensors found in village "${village}"`
      });
    }

    // Get sensors in this village that are NOT mapped to any villager
    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s
       LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE s.village = ? AND vs.id IS NULL
       ORDER BY s.name ASC`,
      [village]
    );

    console.log(`Found ${sensorRows.length} unassigned sensors in ${village}`);

    const sensors = [];

    // For each unassigned sensor, get latest measurement from InfluxDB
    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district, state } = sensor;

      try {
        const dataQuery = `
          from(bucket: "${INFLUX_CONFIG.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> filter(fn: (r) => r.devEUI == "${devEUI}")
            |> sort(columns: ["_time"], desc: true)
            |> limit(n: 1)
        `;

        const data = await queryInfluxDB(dataQuery);

        let latestValue = 'No data';
        let latestTime = '';
        let status = 'Offline';

        if (data.length > 0) {
          latestValue = `${data[0]._field}: ${data[0]._value}`;
          const t = new Date(data[0]._time);
          latestTime = t.toLocaleString();
          const now = new Date();
          const diffSeconds = (now - t) / 1000;
          status = diffSeconds <= 22 ? 'Live' : 'Offline';
        }

        sensors.push({
          devEUI,
          name,
          type,
          village,
          panchayat,
          district,
          state,
          measurement: latestValue,
          time: latestTime,
          status,
          isAssigned: false
        });
      } catch (influxError) {
        console.error(`Error fetching InfluxDB data for sensor ${devEUI}:`, influxError);
        sensors.push({
          devEUI,
          name,
          type,
          village,
          panchayat,
          district,
          state,
          measurement: 'No data',
          time: '',
          status: 'Unknown',
          isAssigned: false
        });
      }
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

    if (!devEUI || !phone) {
      return res.status(400).json({
        success: false,
        error: 'devEUI and phone are required'
      });
    }

    const conn = await db.getConnection();
    await conn.beginTransaction();

    try {
      // Get villager ID
      const [[villager]] = await conn.query(
        `SELECT id FROM villagers WHERE phone = ?`,
        [phone]
      );

      if (!villager) {
        throw new Error('Villager not found with this phone number');
      }

      // Get sensor ID
      const [[sensor]] = await conn.query(
        `SELECT id FROM sensors WHERE devEUI = ?`,
        [devEUI]
      );

      if (!sensor) {
        throw new Error('Sensor not found with this devEUI');
      }

      // Check if already mapped to anyone
      const [[existing]] = await conn.query(
        `SELECT id FROM villager_sensors WHERE sensor_id = ?`,
        [sensor.id]
      );

      if (existing) {
        throw new Error('Sensor is already mapped to another villager');
      }

      // Map sensor to villager
      await conn.query(
        `INSERT INTO villager_sensors (villager_id, sensor_id, phone)
         VALUES (?, ?, ?)`,
        [villager.id, sensor.id, phone]
      );

      await conn.commit();

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

// Debug endpoint to check villages and unassigned sensors
app.get('/api/debug/villages', async (req, res) => {
  try {
    // Get all distinct villages
    const [villages] = await db.query(
      `SELECT DISTINCT village FROM sensors WHERE village IS NOT NULL ORDER BY village`
    );

    // Get count of sensors per village with assignment status
    const [sensorCounts] = await db.query(
      `SELECT 
         s.village, 
         COUNT(*) as total_sensors,
         SUM(CASE WHEN vs.id IS NULL THEN 1 ELSE 0 END) as unassigned_count,
         SUM(CASE WHEN vs.id IS NOT NULL THEN 1 ELSE 0 END) as assigned_count
       FROM sensors s
       LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE s.village IS NOT NULL
       GROUP BY s.village
       ORDER BY s.village`
    );

    // Get all unassigned sensors (for quick check)
    const [unassignedSensors] = await db.query(
      `SELECT s.devEUI, s.name, s.type, s.village
       FROM sensors s
       LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE vs.id IS NULL
       ORDER BY s.village`
    );

    res.json({
      success: true,
      villages: villages.map(v => v.village),
      sensorCounts: sensorCounts,
      totalUnassigned: unassignedSensors.length,
      unassignedSensors: unassignedSensors,
      message: 'Use this to verify village names and sensor assignments'
    });

  } catch (err) {
    console.error('❌ Debug endpoint error:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Get sensor history for graphing
app.get('/api/sensors/:devEUI/history', async (req, res) => {
  try {
    const { devEUI } = req.params;
    const { range } = req.query;

    console.log(`📊 Fetching history for sensor ${devEUI} with range ${range}`);

    let rangeValue = '-24h';
    if (range) {
      rangeValue = range;
    }

    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: ${rangeValue})
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.devEUI == "${devEUI}")
        |> aggregateWindow(every: 1h, fn: mean)
        |> sort(columns: ["_time"], desc: false)
    `;

    const rows = await queryInfluxDB(fluxQuery);
    
    const history = rows.map(row => ({
      time: row._time,
      value: row._value,
      field: row._field
    }));

    res.json({
      success: true,
      history: history,
      count: history.length
    });

  } catch (err) {
    console.error('❌ Error fetching sensor history:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
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
  console.log(`🔧 API:    /api/*`);
  console.log(`📱 Mobile: /api/verify/*, /api/mobile/sensors`);
  console.log(`🏠 Admin:  http://localhost:${PORT}/admin`);
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Available API Endpoints:');
  console.log('   GET  /api/villagers');
  console.log('   GET  /api/sensors');
  console.log('   GET  /api/mobile/sensors?phone=XXXXXXXXXX');
  console.log('   GET  /api/admin/dashboard');
  console.log('   GET  /api/admin/mock-status');
  console.log('   POST /api/admin/mock-enable');
  console.log('   POST /api/admin/mock-disable');
  console.log('   POST /api/admin/mock-generate');
  console.log('   GET  /api/sensors/by-district?district=Kasaragod');
  console.log('   GET  /api/districts');
  console.log('   POST /api/verify/check-phone');
  console.log('   POST /api/verify/send-otp');
  console.log('   POST /api/verify/check-otp');
  console.log('   POST /api/verify/resend-otp');
  console.log('   GET  /api/village-sensors/unassigned?village=NAME');
  console.log('   POST /api/sensors/map');
  console.log('   GET  /api/debug/villages');
  console.log('══════════════════════════════════════════════════════');
  console.log('🎲 Mock Data Generator: Disabled by default');
  console.log('   Enable via Admin Dashboard or POST /api/admin/mock-enable');
  console.log('📊 Sensor types: temperature, humidity, pressure, water_level, etc.');
});
