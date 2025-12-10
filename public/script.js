
const API_BASE_URL = 'https://smart-panchayat.onrender.com';
const API_BASE = API_BASE_URL + '/api';
console.log('API Base:', API_BASE);

console.log('Mobile Debug Info:');
console.log('User Agent:', navigator.userAgent);
console.log('Platform:', navigator.platform);
console.log('Screen:', screen.width, 'x', screen.height);

async function checkAdminSession() {
    const token = localStorage.getItem('admin_token');
    
    if (!token) {
        window.location.href = '/login';
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/check-session`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const data = await response.json();
        
        if (!data.success) {
            localStorage.removeItem('admin_token');
            localStorage.removeItem('admin_data');
            window.location.href = '/login';
        } else {
            // Display admin name
            const adminData = JSON.parse(localStorage.getItem('admin_data') || '{}');
            document.getElementById('adminName').textContent = adminData.name || 'Admin';
        }
    } catch (error) {
        console.error('Session check failed:', error);
        window.location.href = '/login';
    }
}

// Logout function
async function logout() {
    try {
        await fetch(`${API_BASE}/admin/logout`, {
            method: 'POST'
        });
    } catch (error) {
        console.error('Logout error:', error);
    }
    
    // Clear local storage
    localStorage.removeItem('admin_token');
    localStorage.removeItem('admin_data');
    localStorage.removeItem('login_time');
    
    // Redirect to login
    window.location.href = '/login';
}

// Check session when page loads
document.addEventListener('DOMContentLoaded', checkAdminSession);

// Auto-logout after 7.5 hours (before token expires)
setTimeout(() => {
    alert('Your session is about to expire. Please login again.');
    logout();
}, 7.5 * 60 * 60 * 1000);

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
    } else {
        showToast('⚠️ Cannot connect to server. Please wait...', 'warning');
        // Retry after 5 seconds
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
                tbody.innerHTML = '<tr><td colspan="4" class="text-center">No sensors found</td></tr>';
                return;
            }
            
            sensors.slice(0, 5).forEach(sensor => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${sensor.name}</td>
                    <td>${sensor.location}</td>
                    <td><span class="badge bg-success">${sensor.status}</span></td>
                    <td>${sensor.value} ${sensor.unit}</td>
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
                tbody.appendChild(row);
            });
        }
        
        // Update sensors management table
        function updateSensorsTable(sensors) {
            const tbody = document.getElementById('allSensorsTable');
            tbody.innerHTML = '';
            
            if (sensors.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" class="text-center">No sensors found</td></tr>';
                return;
            }
            
            sensors.forEach(sensor => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${sensor.id}</td>
                    <td>${sensor.name}</td>
                    <td>${sensor.type}</td>
                    <td>${sensor.location}</td>
                    <td>${sensor.unit || 'N/A'}</td>
                    <td><span class="badge bg-success">Active</span></td>
                    <td>
                        <button class="btn btn-sm btn-outline-primary">Edit</button>
                        <button class="btn btn-sm btn-outline-danger">Delete</button>
                    </td>
                `;
                tbody.appendChild(row);
            });
        }
        
        // Modal functions
        function showAddVillagerModal() {
            const modal = new bootstrap.Modal(document.getElementById('addVillagerModal'));
            modal.show();
        }
        
        function showAddSensorModal() {
            const modal = new bootstrap.Modal(document.getElementById('addSensorModal'));
            modal.show();
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
      document.getElementById('editFatherName').value = villager.father_name || '';
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
      father_name: document.getElementById('editFatherName').value || '',
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
            const formData = new FormData(form);
            const data = Object.fromEntries(formData);
            
            try {
                const response = await fetch(`${API_BASE}/sensors`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showToast('Sensor added successfully!', 'success');
                    bootstrap.Modal.getInstance(document.getElementById('addSensorModal')).hide();
                    form.reset();
                    
                    loadDashboard();
                    loadAllSensors();
                    loadSensorsForStatus();
                } else {
                    showToast('Error: ' + result.error, 'danger');
                }
            } catch (error) {
                showToast('Failed to add sensor: ' + error.message, 'danger');
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
