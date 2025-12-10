require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const path = require('path');
const app = express();

// ==================== INFLUXDB CONFIGURATION ====================
const INFLUX_CONFIG = {
  url: process.env.INFLUX_URL || 'https://us-east-1-1.aws.cloud2.influxdata.com',
  token: process.env.INFLUX_TOKEN,
  org: process.env.INFLUX_ORG || 'Smart Panchayat Org',
  bucket: process.env.INFLUX_BUCKET || 'smart_panchayat'
};

const PORT = process.env.PORT || 8181;

// Initialize InfluxDB clients
const influxDB = new InfluxDB({ url: INFLUX_CONFIG.url, token: INFLUX_CONFIG.token });
const writeApi = influxDB.getWriteApi(INFLUX_CONFIG.org, INFLUX_CONFIG.bucket);
const queryApi = influxDB.getQueryApi(INFLUX_CONFIG.org);

// ==================== MIDDLEWARE ====================
app.use(cors({
  origin: '*',
  credentials: false,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept']
}));

app.use(express.json());

// Log all requests
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// ==================== ADMIN AUTHENTICATION ====================
const ADMIN_USERS = {
  'admin': {
    password: 'smart@panchayat2024',
    name: 'Super Admin',
    email: 'admin@smartpanchayat.in',
    role: 'super_admin',
    permissions: ['all']
  },
  'panchayat_admin': {
    password: 'panchayat@admin',
    name: 'Panchayat Admin',
    email: 'officer@smartpanchayat.in',
    role: 'admin',
    permissions: ['view_dashboard', 'manage_villagers', 'view_reports']
  }
};

// Login endpoint - ADMIN ONLY
app.post('/api/admin/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    
    console.log('🔐 Admin login attempt:', username);
    
    if (!username || !password) {
      return res.status(400).json({
        success: false,
        error: 'Username and password are required'
      });
    }
    
    const admin = ADMIN_USERS[username];
    
    if (!admin || admin.password !== password) {
      console.log('❌ Invalid credentials for:', username);
      return res.status(401).json({
        success: false,
        error: 'Invalid admin credentials'
      });
    }
    
    const tokenData = {
      username: username,
      role: admin.role,
      name: admin.name,
      timestamp: Date.now(),
      expires: Date.now() + (8 * 60 * 60 * 1000)
    };
    
    const token = Buffer.from(JSON.stringify(tokenData)).toString('base64');
    const { password: _, ...adminWithoutPassword } = admin;
    
    console.log('✅ Admin login successful:', username);
    
    res.json({
      success: true,
      message: 'Admin login successful',
      token: token,
      admin: adminWithoutPassword,
      expiresIn: 28800
    });
    
  } catch (error) {
    console.error('❌ Admin login error:', error);
    res.status(500).json({
      success: false,
      error: 'Login failed'
    });
  }
});

// Logout endpoint
app.post('/api/admin/logout', (req, res) => {
  res.json({
    success: true,
    message: 'Logout successful'
  });
});

// Verify Admin Token Middleware
function authenticateAdmin(req, res, next) {
  try {
    // Skip authentication for public endpoints
    const publicPaths = ['/api/health', '/api/test', '/api/admin/login', '/api/login'];
    if (publicPaths.includes(req.path)) {
      return next();
    }
    
    const authHeader = req.headers['authorization'];
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        success: false,
        error: 'Admin access token required'
      });
    }
    
    const token = authHeader.split(' ')[1];
    const decoded = Buffer.from(token, 'base64').toString('ascii');
    const tokenData = JSON.parse(decoded);
    
    if (Date.now() > tokenData.expires) {
      return res.status(401).json({
        success: false,
        error: 'Admin session expired. Please login again.'
      });
    }
    
    if (!ADMIN_USERS[tokenData.username]) {
      return res.status(403).json({
        success: false,
        error: 'Invalid admin credentials'
      });
    }
    
    req.admin = {
      username: tokenData.username,
      name: tokenData.name,
      role: tokenData.role
    };
    
    console.log(`✅ Admin access: ${tokenData.name} (${tokenData.role})`);
    next();
    
  } catch (error) {
    console.error('❌ Token verification error:', error);
    return res.status(403).json({
      success: false,
      error: 'Invalid admin token'
    });
  }
}

// Check admin session
app.get('/api/admin/check-session', authenticateAdmin, (req, res) => {
  res.json({
    success: true,
    admin: req.admin,
    message: 'Admin session is valid'
  });
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

async function getVillagerFields(aadhaarNumber) {
  try {
    const query = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r.aadhaar_number == "${aadhaarNumber}")
        |> filter(fn: (r) => r._field == "name" or r._field == "phone" or r._field == "status" or r._field == "village" or r._field == "panchayat")
        |> last()
    `;
    
    const result = await queryInfluxDB(query);
    const data = { aadhaar_number: aadhaarNumber };
    
    result.forEach(row => {
      if (row._field && row._value !== undefined) {
        data[row._field] = row._value;
      }
    });
    
    return data;
  } catch (error) {
    console.error('Error getting villager fields:', error);
    return null;
  }
}

// ==================== PUBLIC API ROUTES ====================
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

// ==================== PROTECTED API ROUTES ====================

// ADMIN DASHBOARD - PROTECTED
app.get('/api/admin/dashboard', authenticateAdmin, async (req, res) => {
  try {
    const nameQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r._field == "name")
        |> group()
        |> sort(columns: ["_time"], desc: true)
    `;
    
    const nameResult = await queryInfluxDB(nameQuery);
    const villagersMap = new Map();
    
    nameResult.forEach(row => {
      const aadhaar = row.aadhaar_number;
      const time = new Date(row._time).getTime();
      
      if (!villagersMap.has(aadhaar) || time > villagersMap.get(aadhaar).time) {
        villagersMap.set(aadhaar, {
          time: time,
          aadhaar_number: aadhaar,
          name: row._value,
          village: row.village || '',
          panchayat: row.panchayat || '',
          status: row.status || 'active'
        });
      }
    });
    
    const activeVillagers = Array.from(villagersMap.values())
      .filter(v => v.status !== 'deleted');
    
    const totalVillagers = activeVillagers.length;
    const recentVillagers = [];
    const recentActive = activeVillagers.sort((a, b) => b.time - a.time).slice(0, 5);
    
    for (const data of recentActive) {
      const phoneQuery = `
        from(bucket: "${INFLUX_CONFIG.bucket}")
          |> range(start: -365d)
          |> filter(fn: (r) => r._measurement == "villagers")
          |> filter(fn: (r) => r.aadhaar_number == "${data.aadhaar_number}")
          |> filter(fn: (r) => r._field == "phone")
          |> last()
      `;
      
      const phoneResult = await queryInfluxDB(phoneQuery);
      const phone = phoneResult.length > 0 ? phoneResult[0]._value : '';
      
      recentVillagers.push({
        name: data.name || 'Unknown',
        aadhaar_number: data.aadhaar_number,
        village: data.village || '',
        phone: phone || ''
      });
    }
    
    console.log(`📊 Dashboard accessed by: ${req.admin.name}`);
    
    res.json({
      success: true,
      data: {
        statistics: {
          totalVillagers: totalVillagers,
          totalSensors: 0,
          totalVillages: 1,
          activeAlerts: 0
        },
        recentVillagers: recentVillagers,
        recentSensors: []
      }
    });
  } catch (error) {
    console.error('❌ Dashboard error:', error);
    res.json({
      success: true,
      data: {
        statistics: {
          totalVillagers: 0,
          totalSensors: 0,
          totalVillages: 1,
          activeAlerts: 0
        },
        recentVillagers: [],
        recentSensors: []
      }
    });
  }
});

// VILLAGER MANAGEMENT - PROTECTED
app.get('/api/villagers', authenticateAdmin, async (req, res) => {
  console.log('📥 GET /api/villagers called by:', req.admin.name);
  
  try {
    const nameQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r._field == "name")
        |> group()
        |> sort(columns: ["_time"], desc: true)
    `;
    
    const nameResult = await queryInfluxDB(nameQuery);
    const villagersMap = new Map();
    
    nameResult.forEach(row => {
      const aadhaar = row.aadhaar_number;
      const time = new Date(row._time).getTime();
      
      if (!villagersMap.has(aadhaar) || time > villagersMap.get(aadhaar).time) {
        villagersMap.set(aadhaar, {
          time: time,
          aadhaar_number: aadhaar,
          name: row._value,
          village: row.village || '',
          panchayat: row.panchayat || '',
          status: row.status || 'active'
        });
      }
    });
    
    const villagers = [];
    let idCounter = 1;
    
    for (const [aadhaar, data] of villagersMap) {
      if (data.status !== 'deleted') {
        const phoneQuery = `
          from(bucket: "${INFLUX_CONFIG.bucket}")
            |> range(start: -365d)
            |> filter(fn: (r) => r._measurement == "villagers")
            |> filter(fn: (r) => r.aadhaar_number == "${aadhaar}")
            |> filter(fn: (r) => r._field == "phone")
            |> last()
        `;
        
        const phoneResult = await queryInfluxDB(phoneQuery);
        const phone = phoneResult.length > 0 ? phoneResult[0]._value : '';
        
        villagers.push({
          id: idCounter++,
          aadhaar_number: aadhaar,
          name: data.name || 'Unknown',
          phone: phone || '',
          village: data.village || '',
          panchayat: data.panchayat || ''
        });
      }
    }
    
    console.log(`✅ Found ${villagers.length} active villagers`);
    
    res.json({
      success: true,
      villagers: villagers,
      count: villagers.length
    });
    
  } catch (error) {
    console.error('❌ Error fetching villagers:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch villagers: ' + error.message,
      villagers: []
    });
  }
});

app.get('/api/villagers/:aadhaarNumber', authenticateAdmin, async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    console.log(`📥 GET /api/villagers/${aadhaarNumber} called by: ${req.admin.name}`);

    const data = await getVillagerFields(aadhaarNumber);
    
    if (!data || data.status === 'deleted') {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    const villager = {
      aadhaar_number: aadhaarNumber,
      name: data.name || '',
      phone: data.phone || '',
      village: data.village || '',
      panchayat: data.panchayat || '',
      address: data.address || '',
      father_name: data.father_name || '',
      occupation: data.occupation || '',
      role: data.role || 'villager'
    };

    res.json({
      success: true,
      villager: villager
    });
  } catch (error) {
    console.error('❌ Error fetching villager:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch villager'
    });
  }
});

app.post('/api/villagers', authenticateAdmin, async (req, res) => {
  try {
    const {
      aadhaarNumber,
      name,
      phone,
      village,
      panchayat,
      address,
      fatherName,
      occupation
    } = req.body;

    console.log('📥 POST /api/villagers called by:', req.admin.name, 'Data:', req.body);

    if (!aadhaarNumber || !name || !village || !panchayat) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: Aadhaar, Name, Village, and Panchayat are required'
      });
    }

    if (aadhaarNumber.length !== 12 || !/^\d+$/.test(aadhaarNumber)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid Aadhaar number. Must be 12 digits.'
      });
    }

    const writeSuccess = await writeToInfluxDB('villagers', {
      aadhaar_number: aadhaarNumber,
      village: village,
      panchayat: panchayat,
      status: 'active'
    }, {
      name: name,
      phone: phone || '',
      address: address || '',
      father_name: fatherName || '',
      occupation: occupation || '',
      role: 'villager'
    });
    
    if (!writeSuccess) {
      return res.status(500).json({
        success: false,
        error: 'Failed to save villager to database'
      });
    }

    console.log('✅ New villager added by:', req.admin.name, 'Details:', { aadhaarNumber, name });

    res.json({
      success: true,
      message: 'Villager added successfully',
      data: {
        aadhaar_number: aadhaarNumber,
        name: name,
        phone: phone || '',
        village: village,
        panchayat: panchayat,
        address: address || '',
        father_name: fatherName || '',
        occupation: occupation || ''
      }
    });

  } catch (error) {
    console.error('❌ Error adding villager:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to add villager: ' + error.message
    });
  }
});

app.delete('/api/villagers/:aadhaarNumber', authenticateAdmin, async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    console.log(`🗑️ DELETE /api/villagers/${aadhaarNumber} called by: ${req.admin.name}`);

    if (!aadhaarNumber) {
      return res.status(400).json({
        success: false,
        error: 'Aadhaar number is required'
      });
    }

    const data = await getVillagerFields(aadhaarNumber);
    
    if (!data || data.status === 'deleted') {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    const writeSuccess = await writeToInfluxDB('villagers', {
      aadhaar_number: aadhaarNumber,
      village: data.village || 'unknown',
      panchayat: data.panchayat || 'unknown',
      status: 'deleted'
    }, {
      name: data.name || '',
      phone: data.phone || '',
      deleted: 'true',
      deleted_at: new Date().toISOString()
    });

    if (!writeSuccess) {
      return res.status(500).json({
        success: false,
        error: 'Failed to delete villager from database'
      });
    }

    console.log('✅ Villager marked as deleted by:', req.admin.name, 'Aadhaar:', aadhaarNumber);

    res.json({
      success: true,
      message: 'Villager deleted successfully'
    });

  } catch (error) {
    console.error('❌ Error deleting villager:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to delete villager: ' + error.message
    });
  }
});

app.put('/api/villagers/:aadhaarNumber', authenticateAdmin, async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    const {
      name,
      phone,
      village,
      panchayat,
      address,
      fatherName,
      occupation
    } = req.body;

    console.log('📝 PUT /api/villagers/:aadhaarNumber called by:', req.admin.name, 'Data:', req.body);

    if (!aadhaarNumber) {
      return res.status(400).json({
        success: false,
        error: 'Aadhaar number is required'
      });
    }

    const existingData = await getVillagerFields(aadhaarNumber);
    if (!existingData || existingData.status !== 'active') {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    const updateData = {};
    if (name !== undefined) updateData.name = name;
    if (phone !== undefined) updateData.phone = phone;
    if (village !== undefined) updateData.village = village;
    if (panchayat !== undefined) updateData.panchayat = panchayat;
    if (address !== undefined) updateData.address = address;
    if (fatherName !== undefined) updateData.father_name = fatherName;
    if (occupation !== undefined) updateData.occupation = occupation;

    const writeSuccess = await writeToInfluxDB('villagers', {
      aadhaar_number: aadhaarNumber,
      village: village || existingData.village || 'unknown',
      panchayat: panchayat || existingData.panchayat || 'unknown',
      status: 'active'
    }, {
      ...updateData,
      role: 'villager',
      updated_at: new Date().toISOString()
    });

    if (!writeSuccess) {
      return res.status(500).json({
        success: false,
        error: 'Failed to update villager in database'
      });
    }

    console.log('✅ Villager updated by:', req.admin.name, 'Aadhaar:', aadhaarNumber);

    res.json({
      success: true,
      message: 'Villager updated successfully',
      data: {
        aadhaar_number: aadhaarNumber,
        ...updateData
      }
    });

  } catch (error) {
    console.error('❌ Error updating villager:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to update villager: ' + error.message
    });
  }
});

// ==================== OTHER ENDPOINTS ====================
app.get('/api/sensors', authenticateAdmin, (req, res) => {
  res.json({
    success: true,
    sensors: [],
    count: 0
  });
});

app.post('/api/sensors', authenticateAdmin, (req, res) => {
  res.json({
    success: true,
    message: 'Sensor added (simulated)',
    data: req.body
  });
});

// Legacy login endpoint (for backward compatibility)
app.post('/api/login', (req, res) => {
  const { aadhaarNumber } = req.body;

  if (!aadhaarNumber || aadhaarNumber.length !== 12) {
    return res.status(400).json({
      success: false,
      error: 'Please enter valid 12-digit Aadhaar number'
    });
  }

  const isAdmin = aadhaarNumber === '999999999999';
  
  res.json({
    success: true,
    token: 'token-' + Date.now(),
    user: {
      name: isAdmin ? 'Admin User' : 'Villager User',
      aadhaarNumber: aadhaarNumber,
      role: isAdmin ? 'admin' : 'villager'
    }
  });
});

app.get('/api/debug/raw', authenticateAdmin, async (req, res) => {
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

// ==================== SERVING HTML PAGES ====================
// Serve static files from public directory
app.use(express.static('public'));

// Serve admin page
app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve main page
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
  console.log(`🔐 Admin:  http://localhost:${PORT}/admin`);
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Available API Endpoints:');
  console.log('   POST /api/admin/login    - Admin login');
  console.log('   POST /api/admin/logout   - Admin logout');
  console.log('   GET  /api/admin/dashboard - Admin dashboard (protected)');
  console.log('   GET  /api/villagers      - Get all villagers (protected)');
  console.log('   POST /api/villagers      - Add villager (protected)');
  console.log('   GET  /api/health         - Health check (public)');
  console.log('   GET  /api/test           - Test endpoint (public)');
  console.log('══════════════════════════════════════════════════════');
});