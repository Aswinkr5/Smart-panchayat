const API_BASE = 'https://smart-panchayath.onrender.com/api';
console.log('API Base:', API_BASE);

console.log('Mobile Debug Info:');
console.log('User Agent:', navigator.userAgent);
console.log('Platform:', navigator.platform);
console.log('Screen:', screen.width, 'x', screen.height);

// Test the API immediately
async function testAPIConnection() {
    try {
        console.log('Testing API connection to:', API_BASE + '/health');
        const response = await fetch(API_BASE + '/health');
        const data = await response.json();
        console.log('API Test Success:', data);
        return true;
    } catch (error) {
        console.error('API Test Failed:', error);
        return false;
    }
}

// SINGLE DOMContentLoaded listener
document.addEventListener('DOMContentLoaded', async function() {
    console.log('🚀 Dashboard loading...');
    const apiConnected = await testAPIConnection();

    if (apiConnected) {
        loadDashboard();
        loadAllVillagers();
        loadAllSensors();
        loadSensorsForStatus();

        // 🔄 AUTO REFRESH SENSOR DATA (Option A)
        setInterval(() => {
            loadAllSensors();
            loadSensorsForStatus();
        }, 5000);

    } else {
        showToast('⚠️ Cannot connect to server. Please wait...', 'warning');
        setTimeout(() => location.reload(), 5000);
    }
});


        // Section navigation
        function showSection(section) {
            document.getElementById('dashboardSection').style.display = 'none';
            document.getElementById('villagersSection').style.display = 'none';
            document.getElementById('sensorsSection').style.display = 'none';

            document.getElementById(section + 'Section').style.display = 'block';

            document.querySelectorAll('.sidebar .nav-link').forEach(link => {
                link.classList.remove('active');
            });
            event.target.classList.add('active');
        }

        // Load dashboard data
        async function loadDashboard() {
            try {
                console.log('🔄 Loading dashboard...');
                const response = await fetch(`${API_BASE}/admin/dashboard`);
                const data = await response.json();

                if (data.success) {
                    const stats = data.data.statistics;
                    document.getElementById('totalVillagers').textContent = stats.totalVillagers;
                    document.getElementById('totalSensors').textContent = stats.totalSensors;
                    document.getElementById('totalVillages').textContent = stats.totalVillages;
                    document.getElementById('activeAlerts').textContent = stats.activeAlerts;

                    updateRecentVillagers(data.data.recentVillagers || []);
                }
            } catch (error) {
                console.error('Error loading dashboard:', error);
            }
        }

        // Load all villagers for management section
        async function loadAllVillagers() {
            try {
                console.log('🔄 Loading villagers from:', `${API_BASE}/villagers`);
                const response = await fetch(`${API_BASE}/villagers`);
                const data = await response.json();
                console.log('📥 Villagers API response:', data);

                if (data.success) {
                    updateVillagersTable(data.villagers || []);
                    console.log(`✅ Loaded ${data.villagers.length} villagers`);
                } else {
                    console.error('❌ Failed to load villagers:', data.error);
                    showToast('Failed to load villagers: ' + data.error, 'danger');
                }
            } catch (error) {
                console.error('❌ Error loading villagers:', error);
                showToast('Failed to load villagers. Check server connection.', 'danger');
            }
        }

        // Load all sensors for management section
        async function loadAllSensors() {
            try {
                const response = await fetch(`${API_BASE}/sensors`);
                const data = await response.json();

                if (data.success) {
                    updateSensorsTable(data.sensors || []);
                }
            } catch (error) {
                console.error('Error loading sensors:', error);
            }
        }

        // Load sensors for status table
        async function loadSensorsForStatus() {
            try {
                const response = await fetch(`${API_BASE}/sensors`);
                const data = await response.json();

                if (data.success) {
                    updateSensorStatusTable(data.sensors || []);
                }
            } catch (error) {
                console.error('Error loading sensor status:', error);
            }
        }

        // Update recent villagers table - FIXED
        function updateRecentVillagers(villagers) {
            const tbody = document.getElementById('recentVillagersTable');
            tbody.innerHTML = '';

            if (villagers.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" class="text-center">No villagers found</td></tr>';
                return;
            }

            villagers.slice(0, 5).forEach(villager => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${villager.name || 'N/A'}</td>
                    <td>${villager.aadhaar_number || 'N/A'}</td>
                    <td>${villager.village || 'N/A'}</td>
                    <td>${villager.phone || 'N/A'}</td>
                `;
                tbody.appendChild(row);
            });
        }

        // Update sensor status table
        function updateSensorStatusTable(sensors) {
          const tbody = document.getElementById('sensorStatusTable');
          tbody.innerHTML = '';
       
          if (sensors.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4">No sensors found</td></tr>';
            return;
          }
       
          sensors.slice(0, 5).forEach(sensor => {
            const row = document.createElement('tr');
       
            const badgeClass = sensor.status === 'Live' ? 'bg-success' : 'bg-secondary';
       
            row.innerHTML = `
              <td>${sensor.name}</td>
              <td>—</td>
              <td><span class="badge ${badgeClass}">${sensor.status}</span></td>
              <td>${sensor.measurement}</td>
            `;
            tbody.appendChild(row);
          });
        }
         

        // Update villagers management table
        function updateVillagersTable(villagers) {
            const tbody = document.getElementById('allVillagersTable');
            tbody.innerHTML = '';

            console.log(`📊 Updating table with ${villagers.length} villagers`);

            if (villagers.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" class="text-center">No villagers found</td></tr>';
                return;
            }

            villagers.forEach(villager => {
                const row = document.createElement('tr');
                row.setAttribute('data-id', villager.id);
                row.setAttribute('data-aadhaar', villager.aadhaar_number);
                row.innerHTML = `
                    <td>${villager.id}</td>
                    <td>${villager.name}</td>
                    <td>${villager.aadhaar_number}</td>
                    <td>${villager.phone || 'N/A'}</td>
                    <td>${villager.village || 'N/A'}</td>
                    <td>${villager.panchayat || 'N/A'}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-primary me-2" onclick="editVillager('${villager.aadhaar_number}')">
                            <i class="bi bi-pencil"></i> Edit
                        </button>
                        <button class="btn btn-sm btn-outline-danger" onclick="deleteVillager('${villager.aadhaar_number}')">
                            <i class="bi bi-trash"></i> Delete
                        </button>
                    </td>
                `;
                row.style.cursor = 'pointer';

                row.addEventListener('click', (e) => {
                  if (e.target.tagName === 'BUTTON' || e.target.closest('button')) return;
                  openVillagerSensors(villager.aadhaar_number);
                });

                tbody.appendChild(row);
            });
        }

        //to see sensor that belong to particular villager
        async function openVillagerSensors(aadhaar) {
          try {
            const res = await fetch(`${API_BASE}/villagers/${aadhaar}/sensors`);
            const data = await res.json();
       
            if (!data.success) {
              showToast('Failed to load sensors', 'danger');
              return;
            }
       
            // Villager info
            document.getElementById('villagerSensorInfo').innerHTML = `
              <b>Name:</b> ${data.villager.name}<br>
              <b>Aadhaar:</b> ${data.villager.aadhaar}<br>
              <b>Phone:</b> ${data.villager.phone}<br>
              <b>Village:</b> ${data.villager.village}
            `;
       
            const tbody = document.getElementById('villagerSensorsTable');
            tbody.innerHTML = '';
       
            if (data.sensors.length === 0) {
              tbody.innerHTML = `<tr><td colspan="5">No sensors mapped</td></tr>`;
            }
       
            data.sensors.forEach(s => {
              const badge = s.status === 'Live'
                ? 'bg-success'
                : 'bg-secondary';
       
              tbody.innerHTML += `
                <tr>
                  <td>${s.devEUI}</td>
                  <td>${s.name}</td>
                  <td><span class="badge ${badge}">${s.status}</span></td>
                  <td>${s.measurement}</td>
                  <td>${s.time}</td>
                </tr>
              `;
            });
       
            new bootstrap.Modal(
              document.getElementById('villagerSensorsModal')
            ).show();
       
          } catch (err) {
            console.error(err);
            showToast('Error loading villager sensors', 'danger');
          }
        }
       

        // Update sensors management table
        function updateSensorsTable(sensors) {
          const tbody = document.getElementById('allSensorsTable');
          tbody.innerHTML = '';
       
          if (sensors.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4">No sensors found</td></tr>';
            return;
          }
       
          sensors.forEach(sensor => {
            const row = document.createElement('tr');
       
            row.innerHTML = `
              <td>${sensor.devEUI}</td>
              <td>${sensor.name}</td>
              <td>
                <div>${sensor.measurement || 'No data'}</div>
                <small class="text-muted">${sensor.time || ''}</small>
              </td>
              <td>
                <button class="btn btn-sm btn-outline-primary"
                  onclick="editSensor('${sensor.devEUI}')">Edit</button>
                <button class="btn btn-sm btn-outline-danger"
                  onclick="deleteSensor('${sensor.devEUI}')">Delete</button>
              </td>
            `;
       
            tbody.appendChild(row);
          });
        }
       

        async function deleteSensor(devEUI) {
          if (!confirm(`Are you sure you want to delete sensor ${devEUI}?`)) {
            return;
          }
       
          try {
            const response = await fetch(`${API_BASE}/sensors/${devEUI}`, {
              method: 'DELETE'
            });
       
            const result = await response.json();
       
            if (result.success) {
              showToast('✅ Sensor deleted successfully', 'success');
       
              // Refresh sensor tables
              loadAllSensors();
              loadSensorsForStatus();
              loadDashboard();
       
            } else {
              showToast('❌ ' + result.error, 'danger');
            }
       
          } catch (error) {
            console.error('Delete sensor error:', error);
            showToast('⚠️ Failed to delete sensor', 'warning');
          }
        }
       
         
       
         

        // Modal functions
        function showAddVillagerModal() {
            const modal = new bootstrap.Modal(document.getElementById('addVillagerModal'));
            modal.show();
        }

        function showAddSensorModal() {
          const form = document.getElementById('sensorForm');
          form.reset();
       
          // ensure devEUI is enabled
          form.querySelector('[name="devEUI"]').disabled = false;
       
          const title = document.querySelector('#addSensorModal .modal-title');
          if (title) title.textContent = 'Add New Sensor';
       
          new bootstrap.Modal(
            document.getElementById('addSensorModal')
          ).show();
        }        

        //Edit Sensor
        async function editSensor(devEUI) {
          try {
            const res = await fetch(`${API_BASE}/sensors/${devEUI}`);
            const data = await res.json();
       
            if (!data.success) {
              showToast('Failed to load sensor', 'danger');
              return;
            }
       
            const s = data.sensor;

       
            document.getElementById('editDevEUI').value = s.devEUI;
            document.getElementById('editDeviceName').value = s.name;
            document.getElementById('editVillage').value = s.village || '';
            document.getElementById('editPanchayat').value = s.panchayat || '';
            document.getElementById('editSensorPhone').value = s.phone || '';

       
            new bootstrap.Modal(
              document.getElementById('editSensorModal')
            ).show();
           
           
          } catch (err) {
            console.error(err);
            showToast('Error loading sensor', 'danger');
          }
        }
       
        //Update Sensor
        async function updateSensor() {
          const devEUI = document.getElementById('editDevEUI').value;
       
          const payload = {
            deviceName: document.getElementById('editDeviceName').value,
            village: document.getElementById('editVillage').value,
            panchayat: document.getElementById('editPanchayat').value,
            phone: document.getElementById('editSensorPhone').value

          };
       
          try {
            const res = await fetch(`${API_BASE}/sensors/${devEUI}`, {
              method: 'PUT',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload)
            });
       
            const result = await res.json();
       
            if (result.success) {
              showToast('Sensor updated successfully', 'success');
       
              bootstrap.Modal.getInstance(
                document.getElementById('editSensorModal')
              ).hide();
       
              loadAllSensors();
              loadSensorsForStatus();
              loadDashboard();
       
            } else {
              showToast(result.error, 'danger');
            }
       
          } catch (err) {
            console.error(err);
            showToast('Update failed', 'danger');
          }
        }
        // Save villager
        async function saveVillager() {
            const form = document.getElementById('villagerForm');
            const formData = new FormData(form);
            const data = Object.fromEntries(formData);

            console.log('📤 Saving villager data:', data);

            try {
                const response = await fetch(`${API_BASE}/villagers`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    body: JSON.stringify(data)
                });

                const result = await response.json();
                console.log('✅ Server response:', result);

                if (result.success) {
                    showToast('✅ Villager added successfully!', 'success');
                    bootstrap.Modal.getInstance(document.getElementById('addVillagerModal')).hide();
                    form.reset();

                    setTimeout(() => {
                        loadDashboard();
                        loadAllVillagers();
                    }, 1000);

                } else {
                    showToast('❌ Error: ' + (result.error || 'Unknown error'), 'danger');
                }

            } catch (error) {
                console.error('❌ Save error:', error);
                showToast('⚠️ Failed to add villager. Please try again.', 'warning');
            }
        }

        // Edit villager function
async function editVillager(aadhaarNumber) {
  try {
    console.log(`📝 Editing villager with Aadhaar: ${aadhaarNumber}`);

    const response = await fetch(`${API_BASE}/villagers/${aadhaarNumber}`);
    const data = await response.json();

    console.log('Edit API response:', data);

    if (data.success) {
      const villager = data.villager;
      console.log('Villager data:', villager);

      // Populate the edit form
      document.getElementById('editAadhaarNumber').value = villager.aadhaar_number;
      document.getElementById('editAadhaarDisplay').value = villager.aadhaar_number;
      document.getElementById('editName').value = villager.name || '';
      document.getElementById('editPhone').value = villager.phone || '';
      document.getElementById('editVillage').value = villager.village || '';
      document.getElementById('editPanchayat').value = villager.panchayat || '';
      document.getElementById('editOccupation').value = villager.occupation || '';
      document.getElementById('editAddress').value = villager.address || '';

      // Show the modal
      const modal = new bootstrap.Modal(document.getElementById('editVillagerModal'));
      modal.show();
    } else {
      showToast('❌ Error loading villager: ' + data.error, 'danger');
    }
  } catch (error) {
    console.error('❌ Error in editVillager:', error);
    showToast('⚠️ Failed to load villager data', 'warning');
  }
}

// Update villager function - FIXED
async function updateVillager() {
  try {
    const aadhaarNumber = document.getElementById('editAadhaarNumber').value;

    // Get all form values
    const updateData = {
      name: document.getElementById('editName').value || '',
      phone: document.getElementById('editPhone').value || '',
      village: document.getElementById('editVillage').value || '',
      panchayat: document.getElementById('editPanchayat').value || '',
      address: document.getElementById('editAddress').value || '',
      occupation: document.getElementById('editOccupation').value || ''
    };

    console.log('📤 Updating villager:', aadhaarNumber, updateData);

    // Make PUT request
    const response = await fetch(`${API_BASE}/villagers/${aadhaarNumber}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(updateData)
    });

    const result = await response.json();
    console.log('✅ Update response:', result);

    if (result.success) {
      showToast('✅ Villager updated successfully!', 'success');

      // Hide modal
      const modal = bootstrap.Modal.getInstance(document.getElementById('editVillagerModal'));
      modal.hide();

      // Refresh data
      setTimeout(() => {
        loadDashboard();
        loadAllVillagers();
      }, 500);

    } else {
      showToast('❌ Error: ' + (result.error || 'Update failed'), 'danger');
    }

  } catch (error) {
    console.error('❌ Update error:', error);
    showToast('⚠️ Failed to update villager. Please try again.', 'warning');
  }
}

        // Delete villager function
async function deleteVillager(aadhaarNumber) {
  if (!confirm('Are you sure you want to delete this villager? This action cannot be undone.')) {
    return;
  }

  try {
    console.log(`🗑️ Deleting villager with Aadhaar: ${aadhaarNumber}`);

    const response = await fetch(`${API_BASE}/villagers/${aadhaarNumber}`, {
      method: 'DELETE',
      headers: {
        'Accept': 'application/json'
      }
    });

    const result = await response.json();

    if (result.success) {
      showToast('✅ Villager deleted successfully!', 'success');

      // Immediately remove the row from the table
      const rows = document.querySelectorAll('#allVillagersTable tr');
      rows.forEach(row => {
        if (row.getAttribute('data-aadhaar') === aadhaarNumber) {
          row.style.transition = 'all 0.3s';
          row.style.opacity = '0';
          row.style.height = '0';
          row.style.overflow = 'hidden';
          setTimeout(() => row.remove(), 300);
        }
      });

      // Update dashboard counts
      loadDashboard();

    } else {
      showToast('❌ Error: ' + (result.error || 'Delete failed'), 'danger');
    }

  } catch (error) {
    console.error('❌ Delete error:', error);
    showToast('⚠️ Failed to delete villager. Please try again.', 'warning');
  }
}

        // Save sensor
        async function saveSensor() {
          const form = document.getElementById('sensorForm');
          const data = Object.fromEntries(new FormData(form));
       
          if (!data.devEUI || !data.deviceName) {
            showToast('DevEUI and Device Name are required', 'danger');
            return;
          }
       
          try {
            const res = await fetch(`${API_BASE}/sensors`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(data)
            });
       
            const result = await res.json();
       
            if (result.success) {
              showToast('Sensor added successfully', 'success');
       
              bootstrap.Modal.getInstance(
                document.getElementById('addSensorModal')
              ).hide();
       
              form.reset();
              loadAllSensors();
              loadDashboard();
            } else {
              showToast(result.error, 'danger');
            }
       
          } catch (err) {
            console.error(err);
            showToast('Add sensor failed', 'danger');
          }
        }
       
        // Utility functions
        function refreshDashboard() {
            loadDashboard();
            showToast('Dashboard data refreshed', 'info');
        }

        function generateReport() {
            showToast('Report generation feature coming soon!', 'info');
        }

        function showToast(message, type = 'info') {
            const toastContainer = document.querySelector('.toast-container');
            const toastId = 'toast-' + Date.now();

            const toastHtml = `
                <div id="${toastId}" class="toast align-items-center text-white bg-${type} border-0" role="alert">
                    <div class="d-flex">
                        <div class="toast-body">
                            ${message}
                        </div>
                        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
                    </div>
                </div>
            `;

            toastContainer.innerHTML += toastHtml;
            const toastElement = document.getElementById(toastId);
            const toast = new bootstrap.Toast(toastElement);
            toast.show();

            toastElement.addEventListener('hidden.bs.toast', function () {
                toastElement.remove();
            });
        }

        // Request notification permission
        if ('Notification' in window && Notification.permission === 'default') {
            Notification.requestPermission();
        }
