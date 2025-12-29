require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const path = require('path');
const app = express();

// ==================== INFLUXDB CONFIGURATION ====================
const INFLUX_CONFIG = {
  url: process.env.INFLUX_URL || 'https://us-east-1-1.aws.cloud2.influxdata.com',
  token: process.env.INFLUX_TOKEN, // FROM ENVIRONMENT
  org: process.env.INFLUX_ORG || 'NIl',
  bucket: process.env.INFLUX_BUCKET || 'smart_panchayat'
};

const PORT = process.env.PORT || 8181;

// Initialize InfluxDB clients
const influxDB = new InfluxDB({ url: INFLUX_CONFIG.url, token: INFLUX_CONFIG.token });
const writeApi = influxDB.getWriteApi(INFLUX_CONFIG.org, INFLUX_CONFIG.bucket);
const queryApi = influxDB.getQueryApi(INFLUX_CONFIG.org);

// ==================== OTP STORE ====================
const otpStore = new Map(); // Simple in-memory OTP store

// ==================== MIDDLEWARE ====================

// Update CORS middleware to allow mobile access
app.use(cors({
  origin: '*',  // Allow ALL origins for now (for testing)
  credentials: false,  // Must be false when using '*'
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept']
}));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');

  // Handle preflight requests
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.json());

// Detailed request logging middleware
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  console.log('Headers:', JSON.stringify(req.headers));
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
    console.error('❌ Query error:', error.message);
    return [];
  }
}

// Write to InfluxDB helper
async function writeToInfluxDB(measurement, tags, fields) {
  try {
    const point = new Point(measurement);

    // Add tags
    Object.entries(tags).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        point.tag(key, value.toString());
      }
    });

    // Add fields
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

// Get all fields for a specific villager
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
app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    message: 'Smart Panchayat Backend is running',
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

// ==================== VILLAGER MANAGEMENT ====================

// Get all villagers - WITH PHONE NUMBERS
app.get('/api/villagers', async (req, res) => {
  console.log('📥 GET /api/villagers called');

  try {
    // Get all name entries first
    const nameQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r._field == "name")
        |> group()
        |> sort(columns: ["_time"], desc: true)
    `;

    const nameResult = await queryInfluxDB(nameQuery);

    // Process results - get unique latest entries
    const villagersMap = new Map();

    nameResult.forEach(row => {
      const aadhaar = row.aadhaar_number;
      const time = new Date(row._time).getTime();

      // Only keep the latest entry for each aadhaar
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

    // Get phone numbers for active villagers
    const villagers = [];
    let idCounter = 1;

    for (const [aadhaar, data] of villagersMap) {
      if (data.status !== 'deleted') {
        // Get phone number for this villager
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

// Get a specific villager - WITH ALL FIELDS
app.get('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    console.log(`📥 GET /api/villagers/${aadhaarNumber} called`);

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

// Add new villager
app.post('/api/villagers', async (req, res) => {
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

    console.log('📥 POST /api/villagers called with data:', req.body);

    // Validation
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

    // Write villager data
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

    console.log('✅ New villager added:', { aadhaarNumber, name });

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

// Delete a villager
app.delete('/api/villagers/:aadhaarNumber', async (req, res) => {
  try {
    const { aadhaarNumber } = req.params;
    console.log(`🗑️ DELETE /api/villagers/${aadhaarNumber} called`);

    if (!aadhaarNumber) {
      return res.status(400).json({
        success: false,
        error: 'Aadhaar number is required'
      });
    }

    // Get villager details
    const data = await getVillagerFields(aadhaarNumber);

    if (!data || data.status === 'deleted') {
      return res.status(404).json({
        success: false,
        error: 'Villager not found'
      });
    }

    // Mark as deleted
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

    console.log('✅ Villager marked as deleted:', aadhaarNumber);

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

// Update a villager
app.put('/api/villagers/:aadhaarNumber', async (req, res) => {
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

    console.log('📝 PUT /api/villagers/:aadhaarNumber called:', aadhaarNumber, req.body);

    if (!aadhaarNumber) {
      return res.status(400).json({
        success: false,
        error: 'Aadhaar number is required'
      });
    }

    // Check if villager exists and is active
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

    console.log('✅ Villager updated:', aadhaarNumber);

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

// ==================== ADMIN DASHBOARD ====================

app.get('/api/admin/dashboard', async (req, res) => {
  try {
    // Get all name entries first
    const nameQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r._field == "name")
        |> group()
        |> sort(columns: ["_time"], desc: true)
    `;

    const nameResult = await queryInfluxDB(nameQuery);

    // Process results - get unique latest entries
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

    // Count active villagers
    const activeVillagers = Array.from(villagersMap.values())
      .filter(v => v.status !== 'deleted');

    const totalVillagers = activeVillagers.length;

    // Get phone numbers for recent villagers
    const recentVillagers = [];

    // Get top 5 most recent active villagers
    const recentActive = activeVillagers
      .sort((a, b) => b.time - a.time)
      .slice(0, 5);

    for (const data of recentActive) {
      // Get phone number for this villager
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

// ==================== OTHER ENDPOINTS ====================

app.get('/api/sensors', (req, res) => {
  res.json({
    success: true,
    sensors: [],
    count: 0
  });
});

app.post('/api/sensors', (req, res) => {
  res.json({
    success: true,
    message: 'Sensor added (simulated)',
    data: req.body
  });
});

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

// ==================== MOBILE OTP VERIFICATION ====================

// Check if phone number exists in database
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

    // Query to find villager by phone number
    const query = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r._field == "phone")
        |> filter(fn: (r) => r._value == "${phone}")
        |> last()
    `;

    console.log('📊 Querying InfluxDB for phone:', phone);
    const phoneResult = await queryInfluxDB(query);
    console.log('📊 Phone query result:', phoneResult);

    if (phoneResult.length === 0) {
      console.log('❌ Phone not found in database');
      return res.json({
        success: false,
        error: 'Phone number not registered in our system'
      });
    }

    const aadhaarNumber = phoneResult[0].aadhaar_number;
    console.log('📊 Found Aadhaar:', aadhaarNumber);
    
    const villagerData = await getVillagerFields(aadhaarNumber);
    console.log('📊 Villager data:', villagerData);
    console.log('📊 Status:', villagerData?.status);

    if (!villagerData) {
      console.log('❌ No villager data found');
      return res.json({
        success: false,
        error: 'Villager data not found'
      });
    }

    // TEMPORARY: Remove status check for testing
    // if (!villagerData.status || villagerData.status !== 'active') {
    //   console.log('⚠️ Status check failed:', villagerData.status);
    //   return res.json({
    //     success: false,
    //     error: 'Villager account is not active. Status: ' + (villagerData.status || 'undefined')
    //   });
    // }

    console.log(`✅ Phone verified: ${phone} belongs to ${villagerData.name}`);

    res.json({
      success: true,
      message: 'Phone number verified',
      villager: {
        aadhaar_number: aadhaarNumber,
        name: villagerData.name || 'Villager',
        phone: phone,
        village: villagerData.village || '',
        panchayat: villagerData.panchayat || '',
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

// Generate and send OTP (simulated) - MOBILE-ONLY
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
    const phoneQuery = `
      from(bucket: "${INFLUX_CONFIG.bucket}")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "villagers")
        |> filter(fn: (r) => r._field == "phone")
        |> filter(fn: (r) => r._value == "${phone}")
        |> last()
    `;

    const phoneResult = await queryInfluxDB(phoneQuery);

    if (phoneResult.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Phone number not registered'
      });
    }

    const aadhaarNumber = phoneResult[0].aadhaar_number;
    
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

// Verify OTP - MOBILE-ONLY
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
    const villagerData = await getVillagerFields(otpData.aadhaarNumber);
    
    if (!villagerData) {
      return res.json({
        success: false,
        error: 'Villager data not found'
      });
    }

    // Generate token
    const token = 'villager-' + Date.now() + '-' + phone;
    
    // Clear OTP after successful verification
    otpStore.delete(phone);

    console.log(`✅ OTP verified for phone ${phone}`);

    res.json({
      success: true,
      token: token,
      user: {
        name: villagerData.name || 'Villager',
        aadhaar_number: otpData.aadhaarNumber,
        phone: phone,
        village: villagerData.village || '',
        panchayat: villagerData.panchayat || '',
        role: 'villager',
        can_edit: false
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

// Add this endpoint
app.get('/api/auth/validate', async (req, res) => {
  const token = req.headers.authorization;
  if (!token) {
    return res.json({ success: false, error: 'No token' });
  }
  
  // Simple token validation (in production, use JWT)
  if (token.startsWith('villager-')) {
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

// Resend OTP - MOBILE-ONLY
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

// ==================== DEBUG ENDPOINT ====================

// Debug endpoint
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
  console.log(`📡 Server running on port: ${PORT}`);
  console.log(`🔧 API:    /api/*`);
  console.log(`📱 Mobile: /api/verify/*`);
  console.log('══════════════════════════════════════════════════════');
  console.log('✅ Available API Endpoints:');
  console.log('   POST /api/verify/check-phone');
  console.log('   POST /api/verify/send-otp');
  console.log('   POST /api/verify/check-otp');
  console.log('   POST /api/verify/resend-otp');
  console.log('══════════════════════════════════════════════════════');
});



