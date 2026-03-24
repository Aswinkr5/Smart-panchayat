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

// MySQL Configuration
const mysql = require('mysql2');

console.log('🔧 MySQL Configuration:');

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
    
    await db.query(`
      CREATE TABLE IF NOT EXISTS system_settings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        setting_key VARCHAR(100) UNIQUE NOT NULL,
        setting_value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    
    await db.query(`
      INSERT INTO system_settings (setting_key, setting_value) 
      VALUES ('mock_data_enabled', 'false')
      ON DUPLICATE KEY UPDATE setting_key = setting_key
    `);
    
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

const MOCK_CONFIG = {
  enabled: false,
  interval: 7000,
  timer: null
};

function getRandomValueForType(type, sensorName) {
  const name = sensorName.toLowerCase();
  
  switch(type) {
      case 'temperature': return (20 + Math.random() * 15).toFixed(1);
      case 'humidity': return Math.floor(40 + Math.random() * 50).toString();
      case 'pressure': return Math.floor(980 + Math.random() * 40).toString();
      case 'water_level': return (30 + Math.random() * 70).toFixed(1);
      case 'water_quality': return (6.5 + Math.random() * 2).toFixed(1);
      case 'air_quality': return Math.floor(50 + Math.random() * 150).toString();
      case 'ph': return (6.5 + Math.random() * 2).toFixed(1);
      case 'gas': return Math.floor(10 + Math.random() * 90).toString();
      case 'turbidity': return (1 + Math.random() * 99).toFixed(1);
      case 'chlorine': return (0.5 + Math.random() * 1.5).toFixed(2);
      default:
          if (name.includes('temperature')) return (20 + Math.random() * 15).toFixed(1);
          if (name.includes('humidity')) return Math.floor(40 + Math.random() * 50).toString();
          if (name.includes('water') && name.includes('level')) return (30 + Math.random() * 70).toFixed(1);
          if (name.includes('water') && name.includes('quality')) return (6.5 + Math.random() * 2).toFixed(1);
          if (name.includes('pressure')) return Math.floor(980 + Math.random() * 40).toString();
          if (name.includes('air') && name.includes('quality')) return Math.floor(50 + Math.random() * 150).toString();
          if (name.includes('ph')) return (6.5 + Math.random() * 2).toFixed(1);
          return (10 + Math.random() * 90).toFixed(1);
  }
}

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

async function generateMockData() {
  try {
    const [sensors] = await db.query(
      'SELECT devEUI, name, type FROM sensors ORDER BY id DESC'
    );
    
    if (sensors.length === 0) {
      console.log('⚠️ No sensors found to generate mock data');
      return;
    }
    
    console.log(`🎲 Generating mock data for ${sensors.length} sensors at ${new Date().toLocaleTimeString()}...`);
    let generatedCount = 0;
    
    for (const sensor of sensors) {
      const value = parseFloat(getRandomValueForType(sensor.type, sensor.name));
      const unit = getUnitForType(sensor.type, sensor.name);
      
      let fieldName = sensor.type;
      if (fieldName === 'water_level') fieldName = 'water_level';
      if (fieldName === 'water_quality') fieldName = 'water_quality';
      if (fieldName === 'air_quality') fieldName = 'air_quality';
      
      const point = new Point('sensor_data')
        .tag('devEUI', sensor.devEUI)
        .floatField(fieldName, value);
      
      writeApi.writePoint(point);
      await writeApi.flush();
      
      console.log(`   ✓ ${sensor.name} (${sensor.type}): ${value} ${unit}`);
      generatedCount++;
    }
    
    if (generatedCount > 0) {
      console.log(`✅ Mock data generated successfully for ${generatedCount} sensors`);
    }
    
  } catch (error) {
    console.error('❌ Mock data generation error:', error);
  }
}

app.get('/api/debug/raw-sensor-data/:devEUI', async (req, res) => {
  try {
    const { devEUI } = req.params;
    
    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.devEUI == "${devEUI}")
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: 20)
    `;
    
    const rows = await queryInfluxDB(fluxQuery);
    
    res.json({
      success: true,
      devEUI: devEUI,
      count: rows.length,
      data: rows
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

async function generateHistoricalData() {
  try {
    const [sensors] = await db.query('SELECT devEUI, name, type FROM sensors');
    if (sensors.length === 0) return;
    
    console.log('📊 Generating historical data for past 24 hours...');
    
    for (const sensor of sensors) {
      for (let hour = 1; hour <= 24; hour++) {
        const timestamp = new Date(Date.now() - hour * 3600000);
        const value = getRandomValueForType(sensor.type, sensor.name);
        const unit = getUnitForType(sensor.type, sensor.name);
        
        let fieldName = sensor.type;
        const measurementString = `${fieldName}: ${value} ${unit}`.trim();
        
        const point = new Point('sensor_data')
          .tag('devEUI', sensor.devEUI)
          .floatField(fieldName, parseFloat(value))
          .stringField('measurement', measurementString)
          .timestamp(timestamp);
        
        writeApi.writePoint(point);
      }
      await writeApi.flush();
      console.log(`   ✓ Historical data generated for ${sensor.name}`);
    }
    console.log('✅ Historical data generation complete!');
  } catch (error) {
    console.error('❌ Error generating historical data:', error);
  }
}

async function startMockDataGenerator() {
  try {
      const [rows] = await db.query(
          "SELECT setting_value FROM system_settings WHERE setting_key = 'mock_data_enabled'"
      );
      
      if (rows.length > 0) {
          MOCK_CONFIG.enabled = rows[0].setting_value === 'true';
      }
      
      if (MOCK_CONFIG.enabled && !MOCK_CONFIG.timer) {
          console.log('🎲 Starting mock data generator (every 7 seconds)...');
          await generateMockData();
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
        return { enabled: false, interval: 7 };
    }
}

setTimeout(() => {
    startMockDataGenerator();
}, 5000);

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

app.post('/api/admin/mock-enable', async (req, res) => {
    try {
        const result = await toggleMockData(true);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/mock-disable', async (req, res) => {
    try {
        const result = await toggleMockData(false);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/mock-generate', async (req, res) => {
    try {
        await generateMockData();
        res.json({ success: true, message: 'Mock data generated' });
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
      const flux = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
          |> last()
      `;

      const data = await queryInfluxDB(flux);

      let measurement = 'No data';
      let time = '';
      let status = 'Offline';

      if (data.length > 0) {
        measurement = data[0].measurement || `${data[0]._field}: ${data[0]._value}`;
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

    res.json({ success: true, villager, sensors: result });
  } catch (err) {
    console.error('❌ Error fetching villager sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== SENSOR MANAGEMENT ====================
// IMPORTANT: Specific routes FIRST, then parameter routes

// 1. SPECIFIC ROUTE: Get sensors by district
app.get('/api/sensors/by-district', async (req, res) => {
  try {
    const { district } = req.query;
    if (!district) return res.status(400).json({ success: false, error: 'District name is required' });

    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s
       WHERE s.district = ? 
       ORDER BY s.name ASC`,
      [district]
    );

    const sensors = [];

    for (const sensor of sensorRows) {
      // Check if sensor has a phone number (assigned)
      const [[assignment]] = await db.query(
        `SELECT phone FROM villager_sensors WHERE sensor_id = ?`,
        [sensor.id]
      );
      
      // Sensor is assigned only if there's a phone number
      const isAssigned = assignment !== null && assignment.phone !== null && assignment.phone !== '';
      
      console.log(`Sensor ${sensor.devEUI}: phone = ${assignment?.phone || 'NULL'}, isAssigned = ${isAssigned}`);

      // Get latest reading
      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
          |> last()
      `;

      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';

      if (data.length > 0) {
        if (data[0][sensor.type] !== undefined) {
          latestValue = `${sensor.type}: ${data[0][sensor.type]}`;
        } else if (data[0].measurement) {
          latestValue = data[0].measurement;
        } else if (data[0]._field && data[0]._value) {
          latestValue = `${data[0]._field}: ${data[0]._value}`;
        }
        
        const t = new Date(data[0]._time);
        latestTime = t.toLocaleString();
        const now = new Date();
        const diffSeconds = (now - t) / 1000;
        status = diffSeconds <= 22 ? 'Live' : 'Offline';
      }

      sensors.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        type: sensor.type,
        village: sensor.village || 'Unknown',
        panchayat: sensor.panchayat || 'Unknown',
        district: sensor.district,
        state: sensor.state,
        measurement: latestValue,
        time: latestTime,
        status,
        isAssigned
      });
    }

    res.json({ success: true, district, sensors, count: sensors.length });
  } catch (err) {
    console.error('❌ Error fetching sensors by district:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 2. SPECIFIC ROUTE: Get district statistics
app.get('/api/district-stats/:district', async (req, res) => {
  try {
    const { district } = req.params;
    
    console.log(`📍 Fetching statistics for district: ${district}`);
    
    // Get all sensors in the district that are NOT mapped (no phone number in villager_sensors)
    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s
       LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE s.district = ? AND (vs.phone IS NULL OR vs.phone = '')
       ORDER BY s.type ASC`,
      [district]
    );
    
    console.log(`📊 Found ${sensorRows.length} unmapped sensors in ${district}`);
    
    // Group sensors by type
    const sensorsByType = {
      temperature: [],
      humidity: [],
      water_quality: [],
      air_quality: []
    };
    
    for (const sensor of sensorRows) {
      const sensorType = sensor.type;
      if (sensorsByType[sensorType]) {
        sensorsByType[sensorType].push(sensor);
      }
    }
    
    // Fetch latest values from InfluxDB and calculate averages
    const stats = {};
    
    for (const [type, sensors] of Object.entries(sensorsByType)) {
      if (sensors.length === 0) continue;
      
      let total = 0;
      let validCount = 0;
      const sensorValues = [];
      
      for (const sensor of sensors) {
        const dataQuery = `
          from(bucket: "${INFLUX_CONFIG.bucket}")
            |> range(start: -5m)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
            |> last()
        `;
        
        const data = await queryInfluxDB(dataQuery);
        
        if (data.length > 0) {
          let value = null;
          
          if (data[0][type] !== undefined && typeof data[0][type] === 'number') {
            value = data[0][type];
          } else if (data[0]._value !== undefined && typeof data[0]._value === 'number') {
            value = data[0]._value;
          } else if (data[0].value !== undefined && typeof data[0].value === 'number') {
            value = data[0].value;
          }
          
          if (value !== null && value > 0) {
            total += value;
            validCount++;
            sensorValues.push({
              name: sensor.name,
              village: sensor.village,
              value: value
            });
          }
        }
      }
      
      if (validCount > 0) {
        const average = total / validCount;
        stats[type] = {
          count: validCount,
          totalSensors: sensors.length,
          average: average,
          unit: _getUnitForType(type),
          sensors: sensorValues,
          lastUpdated: new Date().toISOString()
        };
      } else {
        stats[type] = {
          count: 0,
          totalSensors: sensors.length,
          average: null,
          unit: _getUnitForType(type),
          sensors: [],
          lastUpdated: null
        };
      }
    }
    
    console.log(`✅ Calculated stats for ${district}:`, Object.keys(stats));
    
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

function _getUnitForType(type) {
  switch(type) {
    case 'temperature': return '°C';
    case 'humidity': return '%';
    case 'water_quality': return 'pH';
    case 'air_quality': return 'AQI';
    default: return '';
  }
}

// 3. PARAMETER ROUTE: Get single sensor
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
      return res.status(404).json({ success: false, error: 'Sensor not found' });
    }

    res.json({ success: true, sensor: rows[0] });
  } catch (err) {
    console.error('❌ Error fetching sensor:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 4. GENERAL ROUTE: Get all sensors
app.get('/api/sensors', async (req, res) => {
  try {
    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, type, village, panchayat, district, state
       FROM sensors ORDER BY id DESC`
    );

    const sensors = [];

    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district, state } = sensor;

      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${devEUI}")
          |> last()
      `;

      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';

      if (data.length > 0) {
        latestValue = data[0].measurement || `${data[0]._field}: ${data[0]._value}`;
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

// 5. Add new sensor
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
      await conn.query(`INSERT INTO villager_sensors (villager_id, sensor_id, phone) VALUES (?, ?, ?)`, [villager.id, sensorId, phone]);
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

// 6. Update sensor
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
      await conn.query(`INSERT INTO villager_sensors (villager_id, sensor_id, phone) VALUES (?, (SELECT id FROM sensors WHERE devEUI = ?), ?)`, [villager.id, devEUI, phone]);
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

// 7. Delete sensor
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

// ==================== DISTRICT ENDPOINTS ====================

app.get('/api/districts', async (req, res) => {
  try {
    const [rows] = await db.query(`SELECT DISTINCT district FROM sensors WHERE district IS NOT NULL AND district != '' ORDER BY district`);
    const districts = rows.map(row => row.district);
    res.json({ success: true, districts, count: districts.length });
  } catch (err) {
    console.error('❌ Error fetching districts:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== MOBILE ENDPOINTS ====================

app.get('/api/mobile/sensors', async (req, res) => {
  try {
    const { phone } = req.query;
    if (!phone) return res.status(400).json({ success: false, error: 'Phone number is required' });

    const [[villager]] = await db.query(
      `SELECT id, name, phone, village, panchayat, district, state FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (!villager) return res.status(404).json({ success: false, error: 'Villager not found' });

    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s JOIN villager_sensors vs ON vs.sensor_id = s.id WHERE vs.villager_id = ? ORDER BY s.id DESC`,
      [villager.id]
    );

    const sensors = [];

    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district, state } = sensor;

      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${devEUI}")
          |> last()
      `;

      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';

      if (data.length > 0) {
        latestValue = data[0].measurement || `${data[0]._field}: ${data[0]._value}`;
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
      `SELECT devEUI, name, type, village, panchayat, district, state FROM sensors ORDER BY installed_at DESC LIMIT 5`
    );

    const recentSensors = [];
    for (const sensor of sensorRows) {
      const flux = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
          |> last()
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

    const [rows] = await db.query(`SELECT id, aadhaar, name, village, panchayat, district, state FROM villagers WHERE phone = ?`, [phone]);
    if (rows.length === 0) {
      return res.json({ success: false, error: 'Phone number not registered in our system' });
    }

    const villager = rows[0];
    res.json({ success: true, message: 'Phone number verified', villager: { ...villager, phone, role: 'villager' } });
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

    const [rows] = await db.query(`SELECT aadhaar FROM villagers WHERE phone = ?`, [phone]);
    if (rows.length === 0) return res.status(404).json({ success: false, error: 'Phone number not registered' });

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;
    otpStore.set(phone, { otp, expiresAt, phone, aadhaarNumber: rows[0].aadhaar, attempts: 0 });

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

    const [rows] = await db.query(`SELECT aadhaar, name, village, panchayat, district, state FROM villagers WHERE phone = ?`, [phone]);
    if (rows.length === 0) return res.json({ success: false, error: 'Villager data not found' });

    const villager = rows[0];
    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count FROM villager_sensors vs JOIN villagers v ON v.id = vs.villager_id WHERE v.phone = ?`,
      [phone]
    );

    const token = 'villager-' + Date.now() + '-' + phone;
    otpStore.delete(phone);

    res.json({
      success: true,
      token,
      user: {
        name: villager.name,
        aadhaar_number: villager.aadhaar,
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

app.get('/api/debug/raw', async (req, res) => {
  try {
    const query = `from(bucket: "${INFLUX_CONFIG.bucket}") |> range(start: -1h) |> filter(fn: (r) => true) |> limit(n: 50)`;
    const result = await queryInfluxDB(query);
    res.json({ success: true, data: result, count: result.length });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.get('/api/debug/latest-sensor-data', async (req, res) => {
  try {
    const [sensors] = await db.query('SELECT devEUI, name FROM sensors');
    const results = [];
    for (const sensor of sensors) {
      const fluxQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${sensor.devEUI}")
          |> last()
      `;
      const data = await queryInfluxDB(fluxQuery);
      results.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        hasData: data.length > 0,
        latestData: data.length > 0 ? { time: data[0]._time, measurement: data[0].measurement } : null
      });
    }
    res.json({ success: true, data: results });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/debug/sensor-history/:devEUI', async (req, res) => {
  try {
    const { devEUI } = req.params;
    const { range } = req.query;
    const rangeValue = range || '-1h';
    
    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: ${rangeValue})
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.devEUI == "${devEUI}")
        |> sort(columns: ["_time"], desc: false)
    `;
    
    const result = await queryInfluxDB(fluxQuery);
    
    res.json({
      success: true,
      devEUI: devEUI,
      range: rangeValue,
      count: result.length,
      data: result
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/debug/mysql-tables', async (req, res) => {
  try {
    const [tables] = await db.query('SHOW TABLES');
    res.json({ success: true, tables });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.get('/api/debug/table/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const [columns] = await db.query(`DESCRIBE ${tableName}`);
    res.json({ success: true, table: tableName, columns });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ==================== UNASSIGNED SENSORS & MAPPING ====================

app.get('/api/village-sensors/unassigned', async (req, res) => {
  try {
    const { village } = req.query;
    if (!village) return res.status(400).json({ success: false, error: 'Village name is required' });

    const [villageCheck] = await db.query(`SELECT DISTINCT village FROM sensors WHERE village = ?`, [village]);
    if (villageCheck.length === 0) {
      return res.json({ success: true, village, sensors: [], count: 0, message: `No sensors found in village "${village}"` });
    }

    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.type, s.village, s.panchayat, s.district, s.state
       FROM sensors s LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE s.village = ? AND vs.id IS NULL ORDER BY s.name ASC`,
      [village]
    );

    const sensors = [];
    for (const sensor of sensorRows) {
      const { devEUI, name, type, village, panchayat, district, state } = sensor;

      const dataQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.devEUI == "${devEUI}")
          |> last()
      `;
      const data = await queryInfluxDB(dataQuery);

      let latestValue = 'No data';
      let latestTime = '';
      let status = 'Offline';

      if (data.length > 0) {
        latestValue = data[0].measurement || `${data[0]._field}: ${data[0]._value}`;
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
      // Get villager ID
      const [[villager]] = await conn.query(`SELECT id FROM villagers WHERE phone = ?`, [phone]);
      if (!villager) throw new Error('Villager not found with this phone number');

      // Get sensor ID
      const [[sensor]] = await conn.query(`SELECT id FROM sensors WHERE devEUI = ?`, [devEUI]);
      if (!sensor) throw new Error('Sensor not found with this devEUI');

      // Check if already mapped
      const [[existing]] = await conn.query(
        `SELECT id FROM villager_sensors WHERE sensor_id = ?`,
        [sensor.id]
      );
      
      if (existing) {
        throw new Error('Sensor is already mapped to another villager');
      }

      // Map sensor to villager with phone number
      await conn.query(
        `INSERT INTO villager_sensors (villager_id, sensor_id, phone) VALUES (?, ?, ?)`,
        [villager.id, sensor.id, phone]
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

app.get('/api/debug/villages', async (req, res) => {
  try {
    const [villages] = await db.query(`SELECT DISTINCT village FROM sensors WHERE village IS NOT NULL ORDER BY village`);
    const [sensorCounts] = await db.query(`
      SELECT s.village, COUNT(*) as total_sensors,
        SUM(CASE WHEN vs.id IS NULL THEN 1 ELSE 0 END) as unassigned_count,
        SUM(CASE WHEN vs.id IS NOT NULL THEN 1 ELSE 0 END) as assigned_count
      FROM sensors s LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
      WHERE s.village IS NOT NULL GROUP BY s.village ORDER BY s.village
    `);
    const [unassignedSensors] = await db.query(`
      SELECT s.devEUI, s.name, s.type, s.village
      FROM sensors s LEFT JOIN villager_sensors vs ON vs.sensor_id = s.id
      WHERE vs.id IS NULL ORDER BY s.village
    `);
    res.json({ success: true, villages: villages.map(v => v.village), sensorCounts, totalUnassigned: unassignedSensors.length, unassignedSensors });
  } catch (err) {
    console.error('❌ Debug endpoint error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/sensors/:devEUI/history', async (req, res) => {
  try {
    const { devEUI } = req.params;
    const { range } = req.query;
    let rangeValue = range ? range : '-24h';

    console.log(`📊 Fetching history for sensor ${devEUI}, range: ${rangeValue}`);

    const [sensorInfo] = await db.query(
      'SELECT type FROM sensors WHERE devEUI = ?',
      [devEUI]
    );
    const sensorType = sensorInfo.length > 0 ? sensorInfo[0].type : 'temperature';
    
    console.log(`📊 Sensor type: ${sensorType}`);

    const fluxQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: ${rangeValue})
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.devEUI == "${devEUI}")
        |> sort(columns: ["_time"], desc: false)
    `;

    const rows = await queryInfluxDB(fluxQuery);
    
    console.log(`📊 Found ${rows.length} raw records`);
    
    if (rows.length === 0) {
      return res.json({ success: true, history: [], count: 0 });
    }

    const history = [];
    
    for (const row of rows) {
      let value = null;
      
      if (row._field === sensorType && row._value !== undefined) {
        value = parseFloat(row._value);
      }
      else if (row[sensorType] !== undefined && typeof row[sensorType] === 'number') {
        value = row[sensorType];
      }
      else if (row.value !== undefined && typeof row.value === 'number') {
        value = row.value;
      }
      else if (row.measurement && typeof row.measurement === 'string') {
        const match = row.measurement.match(/(\d+(?:\.\d+)?)/);
        if (match) {
          value = parseFloat(match[1]);
        }
      }
      else if (row[sensorType] !== undefined) {
        const val = row[sensorType];
        if (typeof val === 'number') {
          value = val;
        } else if (typeof val === 'string') {
          const num = parseFloat(val);
          if (!isNaN(num)) value = num;
        }
      }
      
      if (value !== null && !isNaN(value) && value > 0) {
        history.push({
          time: row._time,
          value: value,
          field: sensorType
        });
      }
    }

    console.log(`✅ Returning ${history.length} valid numeric data points`);
    if (history.length > 0) {
      console.log(`📊 Values: ${history.slice(0, 10).map(h => h.value).join(', ')}...`);
    }

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
  console.log('   GET  /api/sensors/by-district?district=Kasaragod');
  console.log('   GET  /api/district-stats/:district');
  console.log('   GET  /api/mobile/sensors?phone=XXXXXXXXXX');
  console.log('   GET  /api/admin/dashboard');
  console.log('   GET  /api/admin/mock-status');
  console.log('   POST /api/admin/mock-enable');
  console.log('   POST /api/admin/mock-disable');
  console.log('   POST /api/admin/mock-generate');
  console.log('   GET  /api/districts');
  console.log('   GET  /api/debug/latest-sensor-data');
  console.log('   GET  /api/debug/sensor-history/:devEUI');
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
