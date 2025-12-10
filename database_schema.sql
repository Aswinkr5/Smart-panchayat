-- ============================================
-- SMART PANCHAYAT DATABASE SCHEMA
-- PostgreSQL Schema - Clean Version
-- Admin will add all data through dashboard
-- ============================================

-- ============================================
-- ADMINISTRATION TABLES (Panchayat Office)
-- ============================================

-- 1. Admin Users (Panchayat Staff)
CREATE TABLE admin_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(15),
    panchayat_id INTEGER,
    role VARCHAR(20) DEFAULT 'operator', -- operator, supervisor, admin
    is_active BOOLEAN DEFAULT TRUE,
    permissions JSONB DEFAULT '{
        "manage_villagers": true,
        "manage_sensors": true,
        "view_reports": true,
        "manage_admins": false
    }',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);

-- 2. Panchayats
CREATE TABLE panchayats (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    state VARCHAR(100) DEFAULT 'Maharashtra',
    total_villages INTEGER DEFAULT 0,
    total_sensors INTEGER DEFAULT 0,
    total_villagers INTEGER DEFAULT 0,
    contact_phone VARCHAR(15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Villages
CREATE TABLE villages (
    id SERIAL PRIMARY KEY,
    panchayat_id INTEGER REFERENCES panchayats(id),
    name VARCHAR(100) NOT NULL,
    village_code VARCHAR(20),
    population INTEGER,
    households INTEGER,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    water_source VARCHAR(50),
    electricity_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- VILLAGER MANAGEMENT (Admin can CRUD)
-- ============================================

CREATE TABLE villagers (
    id SERIAL PRIMARY KEY,
    aadhaar_number VARCHAR(12) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    father_name VARCHAR(100),
    phone VARCHAR(15),
    email VARCHAR(100),
    village_id INTEGER REFERENCES villages(id),
    address TEXT,
    family_members JSONB, -- [{name, age, relation}]
    ration_card_number VARCHAR(20),
    occupation VARCHAR(50),
    income_range VARCHAR(30),
    education VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    registered_by INTEGER REFERENCES admin_users(id),
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(aadhaar_number, village_id)
);

-- ============================================
-- SENSOR MANAGEMENT (Admin can CRUD)
-- ============================================

CREATE TABLE sensors_metadata (
    id VARCHAR(50) PRIMARY KEY, -- sensor_001, water_001
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL, -- water_quality, air_quality, temperature, humidity
    sub_type VARCHAR(50), -- ph, turbidity, tds, pm2_5, pm10
    icon VARCHAR(10) DEFAULT '📡',
    unit VARCHAR(20),
    location VARCHAR(200),
    village_id INTEGER REFERENCES villages(id),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    installation_date DATE,
    manufacturer VARCHAR(100),
    model VARCHAR(50),
    serial_number VARCHAR(50),
    
    -- Threshold values for alerts
    min_normal FLOAT,
    max_normal FLOAT,
    min_warning FLOAT,
    max_warning FLOAT,
    min_danger FLOAT,
    max_danger FLOAT,
    
    status VARCHAR(20) DEFAULT 'active', -- active, inactive, maintenance
    last_maintenance DATE,
    notes TEXT,
    added_by INTEGER REFERENCES admin_users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ALERTS & NOTIFICATIONS
-- ============================================

CREATE TABLE sensor_alerts (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES sensors_metadata(id),
    alert_type VARCHAR(30), -- warning, danger, offline
    message TEXT,
    value FLOAT,
    threshold FLOAT,
    village_id INTEGER REFERENCES villages(id),
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_by INTEGER REFERENCES admin_users(id),
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- AUDIT LOG (Track all admin actions)
-- ============================================

CREATE TABLE audit_logs (
    id SERIAL PRIMARY KEY,
    admin_id INTEGER REFERENCES admin_users(id),
    action_type VARCHAR(50), -- create_villager, update_sensor, delete_sensor
    table_name VARCHAR(50),
    record_id VARCHAR(100),
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES for Performance
-- ============================================

CREATE INDEX idx_villagers_village ON villagers(village_id);
CREATE INDEX idx_villagers_aadhaar ON villagers(aadhaar_number);
CREATE INDEX idx_sensors_village ON sensors_metadata(village_id);
CREATE INDEX idx_sensors_type ON sensors_metadata(type);
CREATE INDEX idx_alerts_sensor ON sensor_alerts(sensor_id);
CREATE INDEX idx_alerts_village ON sensor_alerts(village_id);
CREATE INDEX idx_alerts_resolved ON sensor_alerts(is_resolved);
CREATE INDEX idx_audit_admin ON audit_logs(admin_id);
CREATE INDEX idx_audit_created ON audit_logs(created_at);

-- ============================================
-- COMMENTS for Documentation
-- ============================================

COMMENT ON TABLE admin_users IS 'Panchayat office staff who manage the system';
COMMENT ON TABLE panchayats IS 'Panchayat administrative units';
COMMENT ON TABLE villages IS 'Villages under each panchayat';
COMMENT ON TABLE villagers IS 'Villagers registered in the system';
COMMENT ON TABLE sensors_metadata IS 'Sensor device information and metadata';
COMMENT ON TABLE sensor_alerts IS 'Alerts generated by sensor readings';
COMMENT ON TABLE audit_logs IS 'Audit trail of all admin actions';

-- ============================================
-- NO DEFAULT DATA INSERTED
-- All data will be added by admin through dashboard
-- ============================================