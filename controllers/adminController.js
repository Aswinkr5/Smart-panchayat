const pool = require('../config/database');
const bcrypt = require('bcryptjs');

class AdminController {
    // ========== VILLAGER MANAGEMENT ==========
    
    static async addVillager(req, res) {
        try {
            const {
                aadhaarNumber, name, phone, villageId,
                fatherName, email, address, familyMembers,
                rationCardNumber, occupation, incomeRange, education
            } = req.body;
            
            const adminId = req.user.userId;

            // Check if villager already exists
            const existing = await pool.query(
                'SELECT id FROM villagers WHERE aadhaar_number = $1',
                [aadhaarNumber]
            );
            
            if (existing.rows.length > 0) {
                return res.status(400).json({
                    success: false,
                    error: 'Villager with this Aadhaar already exists'
                });
            }

            // Insert new villager
            const result = await pool.query(
                `INSERT INTO villagers (
                    aadhaar_number, name, phone, village_id,
                    father_name, email, address, family_members,
                    ration_card_number, occupation, income_range,
                    education, registered_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING *`,
                [
                    aadhaarNumber, name, phone, villageId,
                    fatherName, email, address, JSON.stringify(familyMembers || []),
                    rationCardNumber, occupation, incomeRange,
                    education, adminId
                ]
            );

            // Log audit
            await pool.query(
                `INSERT INTO audit_logs (admin_id, action_type, table_name, record_id)
                 VALUES ($1, 'create_villager', 'villagers', $2)`,
                [adminId, aadhaarNumber]
            );

            // Update village count
            await pool.query(
                'UPDATE panchayats SET total_villagers = total_villagers + 1 WHERE id = $1',
                [villageId]
            );

            // Emit real-time update
            req.io.emit('villager_added', {
                villageId,
                villager: result.rows[0]
            });

            res.json({
                success: true,
                message: 'Villager added successfully',
                data: result.rows[0]
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    static async updateVillager(req, res) {
        try {
            const { id } = req.params;
            const updates = req.body;
            const adminId = req.user.userId;

            // Get old values for audit
            const oldData = await pool.query(
                'SELECT * FROM villagers WHERE id = $1',
                [id]
            );

            if (oldData.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    error: 'Villager not found'
                });
            }

            // Build dynamic update query
            const setClause = [];
            const values = [];
            let valueIndex = 1;

            Object.keys(updates).forEach(key => {
                if (key !== 'id') {
                    setClause.push(`${key} = $${valueIndex}`);
                    values.push(updates[key]);
                    valueIndex++;
                }
            });

            values.push(id); // For WHERE clause

            const query = `
                UPDATE villagers 
                SET ${setClause.join(', ')}, last_updated = CURRENT_TIMESTAMP
                WHERE id = $${valueIndex}
                RETURNING *
            `;

            const result = await pool.query(query, values);

            // Log audit
            await pool.query(
                `INSERT INTO audit_logs (admin_id, action_type, table_name, record_id, old_values, new_values)
                 VALUES ($1, 'update_villager', 'villagers', $2, $3, $4)`,
                [adminId, id, JSON.stringify(oldData.rows[0]), JSON.stringify(updates)]
            );

            // Emit real-time update
            req.io.emit('villager_updated', {
                villageId: result.rows[0].village_id,
                villager: result.rows[0]
            });

            res.json({
                success: true,
                message: 'Villager updated successfully',
                data: result.rows[0]
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    static async deleteVillager(req, res) {
        try {
            const { id } = req.params;
            const adminId = req.user.userId;

            // Get villager data before deletion
            const villager = await pool.query(
                'SELECT * FROM villagers WHERE id = $1',
                [id]
            );

            if (villager.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    error: 'Villager not found'
                });
            }

            // Soft delete (update is_active to false)
            await pool.query(
                'UPDATE villagers SET is_active = false WHERE id = $1',
                [id]
            );

            // Log audit
            await pool.query(
                `INSERT INTO audit_logs (admin_id, action_type, table_name, record_id, old_values)
                 VALUES ($1, 'delete_villager', 'villagers', $2, $3)`,
                [adminId, id, JSON.stringify(villager.rows[0])]
            );

            // Update village count
            await pool.query(
                'UPDATE panchayats SET total_villagers = total_villagers - 1 WHERE id = $1',
                [villager.rows[0].village_id]
            );

            // Emit real-time update
            req.io.emit('villager_deleted', {
                villageId: villager.rows[0].village_id,
                villagerId: id
            });

            res.json({
                success: true,
                message: 'Villager deleted successfully'
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    // ========== SENSOR MANAGEMENT ==========
    
    static async addSensor(req, res) {
        try {
            const sensorData = req.body;
            const adminId = req.user.userId;

            // Generate unique sensor ID
            const sensorId = `${sensorData.type}_${Date.now()}`;

            const result = await pool.query(
                `INSERT INTO sensors_metadata (
                    id, name, type, sub_type, icon, unit, location,
                    village_id, latitude, longitude, installation_date,
                    manufacturer, model, serial_number,
                    min_normal, max_normal, min_warning, max_warning,
                    min_danger, max_danger, added_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                RETURNING *`,
                [
                    sensorId,
                    sensorData.name,
                    sensorData.type,
                    sensorData.sub_type,
                    sensorData.icon || '📡',
                    sensorData.unit,
                    sensorData.location,
                    sensorData.villageId,
                    sensorData.latitude,
                    sensorData.longitude,
                    sensorData.installationDate,
                    sensorData.manufacturer,
                    sensorData.model,
                    sensorData.serialNumber,
                    sensorData.minNormal,
                    sensorData.maxNormal,
                    sensorData.minWarning,
                    sensorData.maxWarning,
                    sensorData.minDanger,
                    sensorData.maxDanger,
                    adminId
                ]
            );

            // Update sensor count
            await pool.query(
                'UPDATE panchayats SET total_sensors = total_sensors + 1 WHERE id = $1',
                [sensorData.villageId]
            );

            // Log audit
            await pool.query(
                `INSERT INTO audit_logs (admin_id, action_type, table_name, record_id)
                 VALUES ($1, 'create_sensor', 'sensors_metadata', $2)`,
                [adminId, sensorId]
            );

            // Emit real-time update
            req.io.emit('sensor_added', {
                villageId: sensorData.villageId,
                sensor: result.rows[0]
            });

            res.json({
                success: true,
                message: 'Sensor added successfully',
                data: result.rows[0]
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    static async updateSensor(req, res) {
        try {
            const { sensorId } = req.params;
            const updates = req.body;
            const adminId = req.user.userId;

            const result = await pool.query(
                `UPDATE sensors_metadata 
                 SET ${
                    Object.keys(updates).map((key, index) => 
                        `${key} = $${index + 1}`
                    ).join(', ')
                 }, updated_at = CURRENT_TIMESTAMP
                 WHERE id = $${Object.keys(updates).length + 1}
                 RETURNING *`,
                [...Object.values(updates), sensorId]
            );

            if (result.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    error: 'Sensor not found'
                });
            }

            // Emit real-time update
            req.io.emit('sensor_updated', {
                sensorId,
                sensor: result.rows[0]
            });

            res.json({
                success: true,
                message: 'Sensor updated successfully',
                data: result.rows[0]
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    static async deleteSensor(req, res) {
        try {
            const { sensorId } = req.params;
            const adminId = req.user.userId;

            // Get sensor data before deletion
            const sensor = await pool.query(
                'SELECT * FROM sensors_metadata WHERE id = $1',
                [sensorId]
            );

            if (sensor.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    error: 'Sensor not found'
                });
            }

            // Soft delete (update status)
            await pool.query(
                'UPDATE sensors_metadata SET status = $1 WHERE id = $2',
                ['deleted', sensorId]
            );

            // Emit real-time update
            req.io.emit('sensor_deleted', {
                sensorId,
                villageId: sensor.rows[0].village_id
            });

            res.json({
                success: true,
                message: 'Sensor deleted successfully'
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    // ========== DASHBOARD DATA ==========
    
    static async getDashboardData(req, res) {
        try {
            const adminId = req.user.userId;
            const panchayatId = req.user.panchayatId;

            // Get all statistics
            const [
                villagersCount,
                sensorsCount,
                villagesCount,
                alertsCount,
                recentVillagers,
                recentSensors,
                activeAlerts
            ] = await Promise.all([
                pool.query('SELECT COUNT(*) FROM villagers WHERE is_active = true'),
                pool.query('SELECT COUNT(*) FROM sensors_metadata WHERE status = $1', ['active']),
                pool.query('SELECT COUNT(*) FROM villages WHERE panchayat_id = $1', [panchayatId]),
                pool.query('SELECT COUNT(*) FROM sensor_alerts WHERE is_resolved = false'),
                pool.query('SELECT * FROM villagers ORDER BY registered_at DESC LIMIT 10'),
                pool.query('SELECT * FROM sensors_metadata ORDER BY created_at DESC LIMIT 10'),
                pool.query('SELECT * FROM sensor_alerts WHERE is_resolved = false ORDER BY created_at DESC LIMIT 10')
            ]);

            res.json({
                success: true,
                data: {
                    statistics: {
                        totalVillagers: parseInt(villagersCount.rows[0].count),
                        totalSensors: parseInt(sensorsCount.rows[0].count),
                        totalVillages: parseInt(villagesCount.rows[0].count),
                        activeAlerts: parseInt(alertsCount.rows[0].count)
                    },
                    recentVillagers: recentVillagers.rows,
                    recentSensors: recentSensors.rows,
                    activeAlerts: activeAlerts.rows
                }
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }
}

module.exports = AdminController;