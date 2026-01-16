require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const path = require('path');
const app = express();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { JWT_SECRET, authMiddleware, requirePanchayatAdmin } = require('./auth');

// ==================== INFLUXDB CONFIGURATION ====================
const INFLUX_CONFIG = {
  url: process.env.INFLUX_URL || 'https://us-east-1-1.aws.cloud2.influxdata.com',
  token: process.env.INFLUX_TOKEN,
  org: process.env.INFLUX_ORG || 'NIl',
  bucket: process.env.INFLUX_BUCKET || 'smart_panchayat'
};

const PORT = process.env.PORT || 8181;

// Initialize InfluxDB clients
const influxDB = new InfluxDB({ url: INFLUX_CONFIG.url, token: INFLUX_CONFIG.token });
const writeApi = influxDB.getWriteApi(INFLUX_CONFIG.org, INFLUX_CONFIG.bucket);
const queryApi = influxDB.getQueryApi(INFLUX_CONFIG.org);

// ==================== MYSQL CONFIGURATION ====================
const mysql = require('mysql2/promise');

const db = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT,
});

// ==================== MIDDLEWARE ====================
console.log('Influx env check:', {
  url: process.env.INFLUX_URL,
  org: process.env.INFLUX_ORG,
  bucket: process.env.INFLUX_BUCKET,
  tokenLoaded: !!process.env.INFLUX_TOKEN
});

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
  next();
});

// ==================== HELPER FUNCTIONS ====================
async function queryInfluxDB(fluxQuery) {
  try {
    const result = await queryApi.collectRows(fluxQuery);
    return result || [];
  } catch (error) {
    console.error('❌ Query error:', error.message);
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
    console.log(`✅ Written to InfluxDB`);
    return true;
  } catch (error) {
    console.error('❌ Write error:', error.message);
    return false;
  }
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

const SENSOR_ACTIVE_THRESHOLD = 20;

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

function resolvePanchayatId(req) {
  console.log('resolvePanchayatId called with user:', req.user);
  
  // Panchayat admin → forced from JWT
  if (req.user.role === 'panchayat_admin') {
    console.log('Panchayat admin, using panchayatId from JWT:', req.user.panchayatId);
    return req.user.panchayatId;
  }

  // Higher admins (state, district, block) → from query parameter
  const queryPanchayatId = req.query.panchayatId;
  console.log('Higher admin, using panchayatId from query:', queryPanchayatId);
  
  return queryPanchayatId;
}

// ==================== API ROUTES ====================
app.get('/api/test', (req, res) => {
  res.json({
    success: true,
    message: 'API is working!',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    message: 'Smart Panchayat Backend is running',
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

// ==================== VILLAGER MANAGEMENT ====================
// Get all villagers - CORRECTED VERSION
// Get all villagers - DEBUG VERSION
app.get('/api/villagers', authMiddleware, async (req, res) => {
  try {
    console.log('=== GET /api/villagers called ===');
    console.log('Request user:', req.user);
    console.log('Query params:', req.query);
    
    const panchayatId = resolvePanchayatId(req);
    console.log('Resolved panchayatId:', panchayatId);
    
    if (!panchayatId) {
      console.error('No panchayatId provided');
      return res.status(400).json({ 
        success: false, 
        error: 'panchayatId required' 
      });
    }

    // First, check what's in the database
    console.log(`Querying villagers for panchayat_id = ${panchayatId}`);
    
    const [rows] = await db.query(
      `SELECT 
         id,
         aadhaar,
         name,
         phone,
         village,
         panchayat,
         occupation,
         address,
         created_at,
         panchayat_id
       FROM villagers
       WHERE panchayat_id = ?
       ORDER BY created_at DESC`,
      [panchayatId]
    );

    console.log(`Found ${rows.length} villagers in database`);
    console.log('Database rows:', rows);

    // Format response to match expected frontend field names
    const formattedVillagers = rows.map(row => ({
      id: row.id,
      aadhaar_number: row.aadhaar,
      name: row.name,
      phone: row.phone,
      village: row.village,
      panchayat: row.panchayat,
      occupation: row.occupation,
      address: row.address,
      created_at: row.created_at,
      panchayat_id: row.panchayat_id // Keep for debugging
    }));

    console.log('Formatted villagers:', formattedVillagers);

    res.json({ 
      success: true, 
      villagers: formattedVillagers,
      debug: {
        panchayatId,
        count: formattedVillagers.length,
        rawCount: rows.length
      }
    });

  } catch (err) {
    console.error('❌ Error fetching villagers:', err);
    res.status(500).json({ 
      success: false, 
      error: err.message,
      stack: err.stack 
    });
  }
});

// Add new villager - CORRECTED VERSION
app.post(
  '/api/villagers',
  authMiddleware,
  requirePanchayatAdmin,
  async (req, res) => {
    try {
      const { aadhaarNumber, name, phone, village, panchayat, occupation, address } = req.body;
      
      console.log('Received villager data:', req.body);

      const panchayatId = req.user.panchayatId;

      if (!aadhaarNumber || !name || !village) {
        return res.status(400).json({
          success: false,
          error: 'Missing required fields: Aadhaar, Name, and Village are required'
        });
      }

      const [result] = await db.query(
        `INSERT INTO villagers 
         (aadhaar, name, phone, village, panchayat, panchayat_id, occupation, address)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [aadhaarNumber, name, phone || null, village, panchayat || null, panchayatId, occupation || null, address || null]
      );

      console.log('Insert result:', result);

      res.json({
        success: true,
        message: 'Villager added successfully',
        insertId: result.insertId
      });

    } catch (err) {
      console.error('Error adding villager:', err);
      if (err.code === 'ER_DUP_ENTRY') {
        return res.status(409).json({
          success: false,
          error: 'Aadhaar number already exists'
        });
      }
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

// Get a specific villager
app.get('/api/villagers/:aadhaarNumber', authMiddleware, async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;

    const [[villager]] = await db.query(
      `SELECT 
         id,
         aadhaar AS aadhaar_number,
         name,
         phone,
         village,
         panchayat,
         panchayat_id,
         occupation,
         address
       FROM villagers
       WHERE aadhaar = ?`,
      [aadhaarNumber]
    );

    if (!villager) {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    const allowedPanchayat = resolvePanchayatId(req);
    if (villager.panchayat_id !== allowedPanchayat) {
      return res.status(403).json({
        success: false,
        error: 'Access denied'
      });
    }

    delete villager.panchayat_id;

    res.json({
      success: true,
      villager
    });

  } catch (err) {
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// Update a villager
app.put(
  '/api/villagers/:aadhaarNumber',
  authMiddleware,
  requirePanchayatAdmin,
  async (req, res) => {
    try {
      const { aadhaarNumber } = req.params;
      const { name, phone, village, occupation, address } = req.body;

      const panchayatId = req.user.panchayatId;

      const [[villager]] = await db.query(
        `SELECT id FROM villagers
         WHERE aadhaar = ? AND panchayat_id = ?`,
        [aadhaarNumber, panchayatId]
      );

      if (!villager) {
        return res.status(403).json({
          success: false,
          error: 'Villager not found or access denied'
        });
      }

      await db.query(
        `UPDATE villagers
         SET name = ?, phone = ?, village = ?, occupation = ?, address = ?
         WHERE aadhaar = ? AND panchayat_id = ?`,
        [name, phone, village, occupation, address, aadhaarNumber, panchayatId]
      );

      res.json({
        success: true,
        message: 'Villager updated successfully'
      });

    } catch (err) {
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

// Delete a villager
app.delete(
  '/api/villagers/:aadhaarNumber',
  authMiddleware,
  requirePanchayatAdmin,
  async (req, res) => {
    try {
      const { aadhaarNumber } = req.params;
      const panchayatId = req.user.panchayatId;

      const [[villager]] = await db.query(
        `SELECT id FROM villagers
         WHERE aadhaar = ? AND panchayat_id = ?`,
        [aadhaarNumber, panchayatId]
      );

      if (!villager) {
        return res.status(403).json({
          success: false,
          error: 'Villager not found or access denied'
        });
      }

      await db.query(
        `DELETE FROM villagers
         WHERE aadhaar = ? AND panchayat_id = ?`,
        [aadhaarNumber, panchayatId]
      );

      res.json({
        success: true,
        message: 'Villager deleted successfully'
      });

    } catch (err) {
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

// Get villager sensors
app.get('/api/villagers/:aadhaar/sensors', authMiddleware, async (req, res) => {
  try {
    const { aadhaar } = req.params;

    const [[villager]] = await db.query(
      `SELECT id, name, aadhaar, phone, village, panchayat_id
       FROM villagers
       WHERE aadhaar = ?`,
      [aadhaar]
    );

    if (!villager) {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    const allowedPanchayat = resolvePanchayatId(req);
    if (villager.panchayat_id !== allowedPanchayat) {
      return res.status(403).json({
        success: false,
        error: 'Access denied'
      });
    }

    const [sensors] = await db.query(
      `SELECT s.id, s.devEUI, s.name
       FROM sensors s
       JOIN villager_sensors vs ON vs.sensor_id = s.id
       WHERE vs.villager_id = ?`,
      [villager.id]
    );

    const result = [];

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

      result.push({
        devEUI: sensor.devEUI,
        name: sensor.name,
        status: data.length ? 'Live' : 'Offline'
      });
    }

    res.json({
      success: true,
      villager,
      sensors: result
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==================== ADMIN DASHBOARD ====================
app.get('/api/admin/dashboard', authMiddleware, async (req, res) => {
  try {
    const panchayatId = resolvePanchayatId(req);
    if (!panchayatId) {
      return res.status(400).json({ error: 'panchayatId required' });
    }

    const [[{ totalVillagers }]] = await db.query(
      `SELECT COUNT(*) AS totalVillagers FROM villagers WHERE panchayat_id = ?`,
      [panchayatId]
    );

    const [[{ totalVillages }]] = await db.query(
      `SELECT COUNT(DISTINCT village) AS totalVillages FROM villagers WHERE panchayat_id = ?`,
      [panchayatId]
    );

    const totalSensors = await getActiveSensorCount();

    const [recentVillagers] = await db.query(
      `SELECT aadhaar AS aadhaar_number, name, village, phone
       FROM villagers WHERE panchayat_id = ?
       ORDER BY created_at DESC LIMIT 5`,
      [panchayatId]
    );

    res.json({
      success: true,
      data: {
        statistics: {
          totalVillagers,
          totalSensors,
          totalVillages,
          activeAlerts: 0
        },
        recentVillagers
      }
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==================== SENSOR MANAGEMENT ====================
// GET sensors - SINGLE CORRECTED VERSION
app.get('/api/sensors', authMiddleware, async (req, res) => {
  try {
    const panchayatId = resolvePanchayatId(req);
    if (!panchayatId) {
      return res.status(400).json({ error: 'panchayatId required' });
    }

    const [sensorRows] = await db.query(
      `SELECT id, devEUI, name, village, panchayat, installed_at
       FROM sensors
       WHERE panchayat_id = ?
       ORDER BY id DESC`,
      [panchayatId]
    );

    const sensors = [];

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

      sensors.push({
        ...sensor,
        status: data.length ? 'Live' : 'Offline',
        measurement: data.length ? JSON.stringify(data[0]) : 'No data',
        time: data.length ? new Date(data[0]._time).toLocaleString() : ''
      });
    }

    res.json({ success: true, sensors });

  } catch (err) {
    console.error('Error fetching sensors:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET single sensor (for edit)
app.get('/api/sensors/:devEUI', authMiddleware, async (req, res) => {
  try {
    const { devEUI } = req.params;
    const panchayatId = resolvePanchayatId(req);

    const [[sensor]] = await db.query(
      `SELECT id, devEUI, name, village, panchayat 
       FROM sensors 
       WHERE devEUI = ? AND panchayat_id = ?`,
      [devEUI, panchayatId]
    );

    if (!sensor) {
      return res.status(404).json({
        success: false,
        error: 'Sensor not found'
      });
    }

    // Get villager phone if mapped
    const [[mapping]] = await db.query(
      `SELECT v.phone 
       FROM villager_sensors vs
       JOIN villagers v ON v.id = vs.villager_id
       WHERE vs.sensor_id = ?`,
      [sensor.id]
    );

    res.json({
      success: true,
      sensor: {
        ...sensor,
        phone: mapping ? mapping.phone : null
      }
    });

  } catch (err) {
    console.error('Error fetching sensor:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Add sensor - CORRECTED VERSION
// Add new sensor (FOR WEBSITE - NO AUTH)
app.post('/api/sensors', async (req, res) => {
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

// Update sensor
app.put(
  '/api/sensors/:devEUI',
  authMiddleware,
  requirePanchayatAdmin,
  async (req, res) => {
    const { devEUI } = req.params;
    const { deviceName, village, panchayat, phone } = req.body;

    const panchayatId = req.user.panchayatId;
    const conn = await db.getConnection();

    try {
      await conn.beginTransaction();

      const [[sensor]] = await conn.query(
        `SELECT id FROM sensors
         WHERE devEUI = ? AND panchayat_id = ?`,
        [devEUI, panchayatId]
      );

      if (!sensor) {
        throw new Error('Sensor not found or access denied');
      }

      await conn.query(
        `UPDATE sensors
         SET name = ?, village = ?, panchayat = ?
         WHERE devEUI = ? AND panchayat_id = ?`,
        [deviceName, village || null, panchayat || null, devEUI, panchayatId]
      );

      await conn.query(
        `DELETE FROM villager_sensors WHERE sensor_id = ?`,
        [sensor.id]
      );

      if (phone) {
        const [[villager]] = await conn.query(
          `SELECT id FROM villagers
           WHERE phone = ? AND panchayat_id = ?`,
          [phone, panchayatId]
        );

        if (!villager) {
          throw new Error('Villager not found in this panchayat');
        }

        await conn.query(
          `INSERT INTO villager_sensors (villager_id, sensor_id, assigned_at)
           VALUES (?, ?, NOW())`,
          [villager.id, sensor.id]
        );
      }

      await conn.commit();

      res.json({
        success: true,
        message: 'Sensor updated successfully'
      });

    } catch (err) {
      await conn.rollback();
      res.status(400).json({
        success: false,
        error: err.message
      });
    } finally {
      conn.release();
    }
  }
);

// Delete sensor
app.delete(
  '/api/sensors/:devEUI',
  authMiddleware,
  requirePanchayatAdmin,
  async (req, res) => {
    const { devEUI } = req.params;
    const panchayatId = req.user.panchayatId;

    if (!devEUI) {
      return res.status(400).json({
        success: false,
        error: 'devEUI is required'
      });
    }

    const conn = await db.getConnection();

    try {
      await conn.beginTransaction();

      const [[sensor]] = await conn.query(
        `SELECT id FROM sensors
         WHERE devEUI = ? AND panchayat_id = ?`,
        [devEUI, panchayatId]
      );

      if (!sensor) {
        return res.status(403).json({
          success: false,
          error: 'Sensor not found or access denied'
        });
      }

      await conn.query(
        `DELETE FROM villager_sensors WHERE sensor_id = ?`,
        [sensor.id]
      );

      await conn.query(
        `DELETE FROM sensors
         WHERE id = ? AND panchayat_id = ?`,
        [sensor.id, panchayatId]
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
  }
);

// ==================== AUTHENTICATION ====================
app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        success: false,
        error: 'Username and password are required'
      });
    }

    const [[user]] = await db.query(
      'SELECT * FROM users WHERE username = ?',
      [username]
    );

    if (!user) {
      return res.status(401).json({
        success: false,
        error: 'Invalid credentials'
      });
    }

    const passwordMatch = await bcrypt.compare(
      password,
      user.password_hash
    );

    if (!passwordMatch) {
      return res.status(401).json({
        success: false,
        error: 'Invalid credentials'
      });
    }

    const token = jwt.sign(
      {
        id: user.id,
        role: user.role,
        districtId: user.district_id,
        blockId: user.block_id,
        panchayatId: user.panchayat_id
      },
      JWT_SECRET,
      { expiresIn: '8h' }
    );

    res.json({
      success: true,
      token,
      role: user.role,
      districtId: user.district_id,
      blockId: user.block_id,
      panchayatId: user.panchayat_id
    });

  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({
      success: false,
      error: 'Internal server error'
    });
  }
});

// User info endpoint - VERIFY THIS EXISTS
app.get('/api/me', authMiddleware, (req, res) => {
  console.log('/api/me endpoint called, user:', req.user);
  res.json({
    id: req.user.id,
    role: req.user.role,
    districtId: req.user.districtId,
    blockId: req.user.blockId,
    panchayatId: req.user.panchayatId
  });
});

// ==================== LOCATION MANAGEMENT ====================
app.get('/api/districts', authMiddleware, async (_, res) => {
  const [r] = await db.query("SELECT id,name FROM locations WHERE type='DISTRICT'");
  res.json(r);
});

app.get('/api/blocks', authMiddleware, async (req, res) => {
  const districtId = req.user.role === 'district_admin' ? req.user.districtId : req.query.districtId;
  const [r] = await db.query(
    "SELECT id,name FROM locations WHERE type='BLOCK' AND parent_id=?",
    [districtId]
  );
  res.json(r);
});

app.get('/api/panchayats', authMiddleware, async (req, res) => {
  const blockId = req.user.role === 'block_admin' ? req.user.blockId : req.query.blockId;
  const [r] = await db.query(
    "SELECT id,name FROM locations WHERE type='PANCHAYAT' AND parent_id=?",
    [blockId]
  );
  res.json(r);
});

// ==================== DEBUG ====================
app.get(
  '/api/debug/raw',
  authMiddleware,
  async (req, res) => {
    try {
      if (req.user.role !== 'state_admin') {
        return res.status(403).json({
          success: false,
          error: 'Access denied'
        });
      }

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
      console.error('Debug raw error:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
);

// ==================== SERVING HTML PAGES ====================
app.use(express.static('public'));

app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ==================== API 404 HANDLER ====================
app.use('/api', (req, res) => {
  res.status(404).json({
    success: false,
    error: `API endpoint not found: ${req.method} ${req.originalUrl}`
  });
});

// ==================== ERROR HANDLER ====================
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
  console.log(`📡 Server: http://localhost:${PORT}`);
  console.log(`🔧 API:    http://localhost:${PORT}/api`);
  console.log(`🏠 Admin:  http://localhost:${PORT}/admin`);
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Available API Endpoints:');
  console.log('   GET  /api/test');
  console.log('   GET  /api/health');
  console.log('   GET  /api/villagers');
  console.log('   POST /api/villagers');
  console.log('   GET  /api/villagers/:aadhaarNumber');
  console.log('   PUT  /api/villagers/:aadhaarNumber');
  console.log('   DELETE /api/villagers/:aadhaarNumber');
  console.log('   GET  /api/sensors');
  console.log('   GET  /api/sensors/:devEUI');
  console.log('   POST /api/sensors');
  console.log('   PUT  /api/sensors/:devEUI');
  console.log('   DELETE /api/sensors/:devEUI');
  console.log('   GET  /api/admin/dashboard');
  console.log('   GET  /api/debug/raw');
  console.log('══════════════════════════════════════════════════════');
});
