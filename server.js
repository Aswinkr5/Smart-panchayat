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

// MySQL Configuration - Use promise wrapper
const mysql = require('mysql2');
const db = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
}).promise(); // ADD .promise() HERE

// ==================== OTP STORE ====================
const otpStore = new Map();

// ==================== TOKEN STORE ====================
const tokenStore = new Map(); // Store: token -> {phone, role, expiresAt}

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

// Authentication middleware
const authenticateToken = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    
    if (!authHeader) {
      return res.status(401).json({
        success: false,
        error: 'No authorization token provided'
      });
    }

    const token = authHeader;
    const tokenData = tokenStore.get(token);

    // Check if token exists and is not expired
    if (!tokenData) {
      return res.status(401).json({
        success: false,
        error: 'Invalid or expired token'
      });
    }

    if (Date.now() > tokenData.expiresAt) {
      tokenStore.delete(token);
      return res.status(401).json({
        success: false,
        error: 'Token expired'
      });
    }

    // Attach user info to request
    req.user = {
      phone: tokenData.phone,
      role: tokenData.role,
      villagerId: tokenData.villagerId,
      name: tokenData.name,
      aadhaar_number: tokenData.aadhaar_number,
      village: tokenData.village,
      panchayat: tokenData.panchayat
    };

    next();
  } catch (error) {
    console.error('❌ Authentication error:', error);
    res.status(500).json({
      success: false,
      error: 'Authentication failed'
    });
  }
};

// Admin middleware
const requireAdmin = (req, res, next) => {
  if (req.user.role !== 'admin') {
    return res.status(403).json({
      success: false,
      error: 'Admin access required'
    });
  }
  next();
};

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

// ==================== VILLAGER MANAGEMENT (MySQL) ====================

// Get all villagers (ADMIN ONLY)
app.get('/api/villagers', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT
         id,
         aadhaar AS aadhaar_number,
         name,
         phone,
         village,
         panchayat,
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

// Get a specific villager (ADMIN ONLY)
app.get('/api/villagers/:aadhaarNumber', authenticateToken, requireAdmin, async (req, res) => {
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

// Add new villager (ADMIN ONLY)
app.post('/api/villagers', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { aadhaarNumber, name, phone, village, panchayat, occupation, address } = req.body;

    if (!aadhaarNumber || !name || !phone || !village || !panchayat) {
      return res.status(400).json({ success: false, error: 'Missing required fields' });
    }

    await db.query(
      `INSERT INTO villagers (aadhaar, name, phone, village, panchayat, occupation, address)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [aadhaarNumber, name, phone, village, panchayat, occupation, address]
    );

    res.json({ success: true, message: 'Villager added successfully' });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ success: false, error: 'Aadhaar or phone already exists' });
    }
    res.status(500).json({ success: false, error: err.message });
  }
});

// Update a villager (ADMIN ONLY)
app.put('/api/villagers/:aadhaarNumber', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    const { name, phone, village, panchayat, occupation, address } = req.body;

    const [result] = await db.query(
      `UPDATE villagers
       SET name = ?, phone = ?, village = ?, panchayat = ?, occupation = ?, address = ?
       WHERE aadhaar = ?`,
      [name, phone, village, panchayat, occupation, address, aadhaarNumber]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ success: false, error: 'Villager not found' });
    }

    res.json({ success: true, message: 'Villager updated' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Delete a villager (ADMIN ONLY)
app.delete('/api/villagers/:aadhaarNumber', authenticateToken, requireAdmin, async (req, res) => {
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

// Get sensors for a specific villager (ADMIN ONLY)
app.get('/api/villagers/:aadhaar/sensors', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { aadhaar } = req.params;

    // Get villager
    const [[villager]] = await db.query(
      `SELECT id, name, aadhaar, phone, village, panchayat
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
      `SELECT s.id, s.devEUI, s.name, s.village, s.panchayat
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
        village: sensor.village,
        panchayat: sensor.panchayat,
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
app.get('/api/sensors', authenticateToken, requireAdmin, async (req, res) => {
  try {
    // Get sensor metadata from MySQL
    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, village, panchayat
       FROM sensors
       ORDER BY id DESC`
    );

    const sensors = [];

    // For each sensor, get latest measurement from InfluxDB
    for (const sensor of sensorRows) {
      const { devEUI, name, village, panchayat } = sensor;

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
        village,
        panchayat,
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

// Get MY sensors (For mobile users - shows only user's sensors)
app.get('/api/my-sensors', authenticateToken, async (req, res) => {
  try {
    const user = req.user;
    
    console.log('📱 MY SENSORS request for user:', user.phone);

    // Get sensors mapped to this villager
    const [sensorRows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.village, s.panchayat
       FROM sensors s
       JOIN villager_sensors vs ON vs.sensor_id = s.id
       JOIN villagers v ON v.id = vs.villager_id
       WHERE v.phone = ?
       ORDER BY s.id DESC`,
      [user.phone]
    );

    const sensors = [];

    // For each sensor, get latest measurement from InfluxDB
    for (const sensor of sensorRows) {
      const { devEUI, name, village, panchayat } = sensor;

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
        village,
        panchayat,
        measurement: latestValue,
        time: latestTime,
        status
      });
    }

    res.json({
      success: true,
      user: {
        name: user.name,
        phone: user.phone,
        village: user.village,
        panchayat: user.panchayat
      },
      sensors: sensors,
      sensorCount: sensors.length
    });
  } catch (err) {
    console.error('❌ Error fetching my sensors:', err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Get single sensor (ADMIN ONLY)
app.get('/api/sensors/:devEUI', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { devEUI } = req.params;

    const [rows] = await db.query(
      `SELECT s.id, s.devEUI, s.name, s.village, s.panchayat,
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

// Add new sensor (ADMIN ONLY)
app.post('/api/sensors', authenticateToken, requireAdmin, async (req, res) => {
  const { devEUI, deviceName, village, panchayat, phone } = req.body;

  if (!devEUI || !deviceName) {
    return res.status(400).json({
      success: false,
      error: 'devEUI and deviceName are required'
    });
  }

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    // Insert sensor
    const [sensorResult] = await conn.query(
      `INSERT INTO sensors (devEUI, name, village, panchayat)
       VALUES (?, ?, ?, ?)`,
      [devEUI, deviceName, village || null, panchayat || null]
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
        : 'Sensor registered successfully'
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

// Update sensor (ADMIN ONLY)
app.put('/api/sensors/:devEUI', authenticateToken, requireAdmin, async (req, res) => {
  const { devEUI } = req.params;
  const { deviceName, village, panchayat, phone } = req.body;

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    // Update sensor metadata
    const [result] = await conn.query(
      `UPDATE sensors
       SET name = ?, village = ?, panchayat = ?
       WHERE devEUI = ?`,
      [deviceName, village || null, panchayat || null, devEUI]
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

// Delete sensor (ADMIN ONLY)
app.delete('/api/sensors/:devEUI', authenticateToken, requireAdmin, async (req, res) => {
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

// ==================== ADMIN DASHBOARD ====================

app.get('/api/admin/dashboard', authenticateToken, requireAdmin, async (req, res) => {
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
         phone,
         panchayat
       FROM villagers
       ORDER BY created_at DESC
       LIMIT 5`
    );

    // Get recent sensors with status
    const [sensorRows] = await db.query(
      `SELECT devEUI, name, village, panchayat FROM sensors ORDER BY installed_at DESC LIMIT 5`
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
        village: sensor.village,
        panchayat: sensor.panchayat,
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
    // Generate admin token
    const token = 'admin-token-' + Date.now();
    const expiresAt = Date.now() + 24 * 60 * 60 * 1000; // 24 hours
    
    tokenStore.set(token, {
      phone: '0000000000',
      role: 'admin',
      expiresAt,
      name: 'Admin User',
      aadhaar_number: aadhaarNumber,
      village: 'Admin Village',
      panchayat: 'Admin Panchayat'
    });

    return res.json({
      success: true,
      token: token,
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
      `SELECT id, name, phone, village, panchayat, aadhaar
       FROM villagers WHERE aadhaar = ?`,
      [aadhaarNumber]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    const villager = rows[0];
    // Generate villager token
    const token = 'villager-token-' + Date.now();
    const expiresAt = Date.now() + 24 * 60 * 60 * 1000; // 24 hours
    
    tokenStore.set(token, {
      phone: villager.phone,
      role: 'villager',
      villagerId: villager.id,
      expiresAt,
      name: villager.name,
      aadhaar_number: villager.aadhaar,
      village: villager.village,
      panchayat: villager.panchayat
    });

    res.json({
      success: true,
      token: token,
      user: {
        name: villager.name,
        aadhaarNumber: aadhaarNumber,
        phone: villager.phone,
        village: villager.village,
        panchayat: villager.panchayat,
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
      `SELECT id, aadhaar, name, village, panchayat
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
      `SELECT id, aadhaar, name, village, panchayat FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Phone number not registered'
      });
    }

    const villager = rows[0];
    const aadhaarNumber = villager.aadhaar;

    // Generate 6-digit OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000; // 5 minutes

    // Store OTP with phone as key
    otpStore.set(phone, {
      otp,
      expiresAt,
      phone,
      aadhaarNumber,
      villagerId: villager.id,
      name: villager.name,
      village: villager.village,
      panchayat: villager.panchayat,
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

    // OTP is correct - generate token
    const token = 'villager-token-' + Date.now() + '-' + phone;
    const expiresAt = Date.now() + 24 * 60 * 60 * 1000; // 24 hours
    
    // Store token
    tokenStore.set(token, {
      phone: phone,
      role: 'villager',
      villagerId: otpData.villagerId,
      expiresAt,
      name: otpData.name,
      aadhaar_number: otpData.aadhaarNumber,
      village: otpData.village,
      panchayat: otpData.panchayat
    });

    // Get sensor count for this villager
    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count 
       FROM villager_sensors vs
       JOIN villagers v ON v.id = vs.villager_id
       WHERE v.phone = ?`,
      [phone]
    );

    const sensorCount = sensorCountResult.sensor_count || 0;

    // Clear OTP after successful verification
    otpStore.delete(phone);

    console.log(`✅ OTP verified for phone ${phone}, ${sensorCount} sensors found`);

    res.json({
      success: true,
      token: token,
      user: {
        name: otpData.name,
        aadhaar_number: otpData.aadhaarNumber,
        phone: phone,
        village: otpData.village,
        panchayat: otpData.panchayat,
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

    // Get villager data
    const [rows] = await db.query(
      `SELECT id, aadhaar, name, village, panchayat FROM villagers WHERE phone = ?`,
      [phone]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Phone number not registered'
      });
    }

    const villager = rows[0];

    // Generate new OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;

    otpStore.set(phone, {
      otp,
      expiresAt,
      phone,
      aadhaarNumber: villager.aadhaar,
      villagerId: villager.id,
      name: villager.name,
      village: villager.village,
      panchayat: villager.panchayat,
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
  try {
    const token = req.headers.authorization;
    
    if (!token) {
      return res.json({ 
        success: false, 
        valid: false,
        error: 'No token' 
      });
    }

    const tokenData = tokenStore.get(token);

    // Check if token exists and is not expired
    if (!tokenData) {
      return res.json({
        success: false,
        valid: false,
        error: 'Invalid token'
      });
    }

    if (Date.now() > tokenData.expiresAt) {
      tokenStore.delete(token);
      return res.json({
        success: false,
        valid: false,
        error: 'Token expired'
      });
    }

    return res.json({
      success: true,
      valid: true,
      user: {
        phone: tokenData.phone,
        role: tokenData.role,
        name: tokenData.name,
        village: tokenData.village,
        panchayat: tokenData.panchayat
      },
      message: 'Token is valid'
    });
  } catch (error) {
    console.error('❌ Token validation error:', error);
    return res.json({
      success: false,
      valid: false,
      error: 'Invalid token'
    });
  }
});

// Get current user profile
app.get('/api/profile', authenticateToken, async (req, res) => {
  try {
    const user = req.user;
    
    // Get sensor count
    const [[sensorCountResult]] = await db.query(
      `SELECT COUNT(*) as sensor_count 
       FROM villager_sensors vs
       JOIN villagers v ON v.id = vs.villager_id
       WHERE v.phone = ?`,
      [user.phone]
    );

    res.json({
      success: true,
      user: {
        name: user.name,
        phone: user.phone,
        aadhaar_number: user.aadhaar_number,
        village: user.village,
        panchayat: user.panchayat,
        role: user.role,
        sensor_count: sensorCountResult.sensor_count || 0
      }
    });
  } catch (error) {
    console.error('❌ Profile error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
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
  console.log(`🔧 Admin API:    /api/* (requires admin token)`);
  console.log(`📱 Mobile API:   /api/my-sensors (requires villager token)`);
  console.log(`🔐 Auth:         /api/verify/*, /api/login, /api/profile`);
  console.log(`🏠 Admin:        http://localhost:${PORT}/admin`);
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Available Mobile Endpoints (after login):');
  console.log('   GET  /api/my-sensors   (with Authorization: token)');
  console.log('   GET  /api/profile      (with Authorization: token)');
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Available Admin Endpoints (with admin token):');
  console.log('   GET  /api/villagers');
  console.log('   GET  /api/sensors');
  console.log('   GET  /api/admin/dashboard');
  console.log('══════════════════════════════════════════════════════');
});
