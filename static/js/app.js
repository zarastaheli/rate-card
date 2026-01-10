// Global state
let currentStep = 1;
let jobId = null;
let uploadData = null;
let mappingData = {};
let selectedServices = [];

const SERVICE_LEVELS = [
    'UPS® Ground',
    'DHL Parcel International Direct - DDU',
    'DHL SM Parcel Expedited',
    'USPS Ground Advantage',
    'UPS 2nd Day Air®',
    'DHL SM Parcel Expedited Max'
];

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initializeStep1();
    updateProgressIndicator();
});

function updateProgressIndicator() {
    document.querySelectorAll('.step').forEach((step, index) => {
        const stepNum = index + 1;
        step.classList.remove('active', 'completed');
        if (stepNum < currentStep) {
            step.classList.add('completed');
        } else if (stepNum === currentStep) {
            step.classList.add('active');
        }
    });
}

function showStep(stepNum) {
    document.querySelectorAll('.step-content').forEach(el => {
        el.style.display = 'none';
    });
    document.getElementById(`step${stepNum}`).style.display = 'block';
    currentStep = stepNum;
    updateProgressIndicator();
}

// Step 1: Upload
function initializeStep1() {
    const fileInput = document.getElementById('fileInput');
    const uploadArea = document.getElementById('uploadArea');
    const fileUploaded = document.getElementById('fileUploaded');
    const replaceFile = document.getElementById('replaceFile');
    const continueBtn = document.getElementById('continueBtn1');
    const existingCustomer = document.getElementById('existingCustomer');
    const merchantIdGroup = document.getElementById('merchantIdGroup');
    const merchantId = document.getElementById('merchantId');

    uploadArea.addEventListener('click', () => fileInput.click());
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.style.borderColor = '#999';
    });
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.style.borderColor = '#ddd';
    });
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.style.borderColor = '#ddd';
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            handleFileSelect(files[0]);
        }
    });

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            handleFileSelect(e.target.files[0]);
        }
    });

    replaceFile.addEventListener('click', (e) => {
        e.preventDefault();
        fileInput.click();
    });

    existingCustomer.addEventListener('change', () => {
        if (existingCustomer.checked) {
            merchantIdGroup.style.display = 'block';
            merchantId.required = true;
        } else {
            merchantIdGroup.style.display = 'none';
            merchantId.required = false;
        }
    });

    continueBtn.addEventListener('click', async () => {
        const merchantName = document.getElementById('merchantName').value;
        if (!merchantName) {
            alert('Please enter merchant name');
            return;
        }
        if (existingCustomer.checked && !merchantId.value) {
            alert('Please enter merchant ID');
            return;
        }
        if (uploadData && uploadData.requires_origin_zip) {
            const originZip = document.getElementById('originZip').value;
            if (!originZip) {
                alert('Please enter origin ZIP');
                return;
            }
        }
        await proceedToMapping();
    });

    document.getElementById('cancelBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to cancel?')) {
            location.reload();
        }
    });
}

async function handleFileSelect(file) {
    if (!file.name.endsWith('.csv')) {
        alert('Please upload a CSV file');
        return;
    }

    const formData = new FormData();
    formData.append('invoice', file);

    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Upload failed');
        }

        uploadData = await response.json();
        jobId = uploadData.job_id;

        // Update UI - show file uploaded
        document.getElementById('fileName').textContent = file.name;
        document.getElementById('uploadArea').style.display = 'none';
        document.getElementById('fileUploaded').style.display = 'flex';
        
        // Show merchant info sections after upload
        document.getElementById('uploadedInfo').style.display = 'block';
        document.getElementById('detectedStructure').textContent = 
            uploadData.detected_structure === 'zone' ? 'Zone-based' : 'Zip-based';
        
        if (uploadData.merchant_name_suggestion) {
            document.getElementById('merchantName').value = uploadData.merchant_name_suggestion;
        }

        if (uploadData.requires_origin_zip) {
            document.getElementById('originZipGroup').style.display = 'block';
            document.getElementById('originZip').required = true;
        }

        document.getElementById('continueBtn1').disabled = false;
    } catch (error) {
        alert('Error uploading file: ' + error.message);
    }
}

async function proceedToMapping() {
    // Save merchant info to session/memory
    showStep(2);
    await loadMappingStep();
}

// Step 2: Mapping
async function loadMappingStep() {
    if (!uploadData) return;

    const mappingTable = document.getElementById('mappingTable');
    mappingTable.innerHTML = '';

    const header = document.createElement('div');
    header.className = 'mapping-header';
    header.innerHTML = `
        <div>Standard field name</div>
        <div>Invoice column</div>
        <div class="status-col">Status</div>
    `;
    mappingTable.appendChild(header);

    const requiredFields = [
        'Order Number',
        'Order Date',
        'Zip',
        'Weight (oz)',
        'Shipping Carrier',
        'Shipping Service',
        'Package Height',
        'Package Width',
        'Package Length',
        'Zone'
    ];
    const optionalFields = ['Label Cost'];

    // Create mapping rows
    [...requiredFields, ...optionalFields].forEach(field => {
        const row = document.createElement('div');
        row.className = 'mapping-row';
        row.dataset.field = field;

        const isRequired = requiredFields.includes(field);
        const suggested = suggestMapping(field, uploadData.columns);

        const fieldName = document.createElement('div');
        fieldName.className = `field-name ${isRequired ? 'required' : ''}`;
        fieldName.textContent = (isRequired ? '* ' : '') + field;

        const selectWrapper = document.createElement('div');
        const select = document.createElement('select');
        select.className = 'mapping-select';
        select.dataset.field = field;
        const errorMessage = document.createElement('div');
        errorMessage.className = 'error-message';
        errorMessage.textContent = 'Required field.';

        // Add empty option first
        const emptyOption = document.createElement('option');
        emptyOption.value = '';
        emptyOption.textContent = 'Select column';
        select.appendChild(emptyOption);

        // Add suggested option with star if there's a suggestion
        if (suggested) {
            const suggestedOption = document.createElement('option');
            suggestedOption.value = suggested;
            suggestedOption.textContent = `★ ${suggested}`;
            suggestedOption.style.fontWeight = 'bold';
            select.appendChild(suggestedOption);
            
            // Add separator
            const separator = document.createElement('option');
            separator.disabled = true;
            separator.textContent = '──────────';
            select.appendChild(separator);
        }

        // Add all column options (excluding suggested if it exists)
        uploadData.columns.forEach(col => {
            if (suggested && col === suggested) {
                return; // Skip, already added as suggestion
            }
            const option = document.createElement('option');
            option.value = col;
            option.textContent = col;
            select.appendChild(option);
        });

        select.addEventListener('change', () => {
            if (select.value) {
                mappingData[field] = select.value;
                row.classList.remove('error');
                updateMappingStatus(row, field, true);
            } else {
                delete mappingData[field];
                if (isRequired) {
                    row.classList.add('error');
                    updateMappingStatus(row, field, false);
                }
            }
            if (isRequired) {
                errorMessage.style.display = select.value ? 'none' : 'block';
            }
            validateMapping();
        });

        selectWrapper.appendChild(select);
        if (isRequired) {
            errorMessage.style.display = 'block';
            selectWrapper.appendChild(errorMessage);
        }
        
        // Don't auto-select - fields start as unmapped
        if (isRequired) {
            row.classList.add('error');
        }

        const status = document.createElement('div');
        status.className = 'status not-mapped';
        status.textContent = 'Not mapped';
        status.dataset.status = field;

        row.appendChild(fieldName);
        row.appendChild(selectWrapper);
        row.appendChild(status);

        mappingTable.appendChild(row);
    });

    validateMapping();

    document.getElementById('backBtn2').addEventListener('click', () => showStep(1));
    document.getElementById('continueBtn2').addEventListener('click', async () => {
        if (!validateMapping()) return;
        
        const labelCostMapped = 'Label Cost' in mappingData;
        if (!labelCostMapped) {
            showLabelCostModal();
            return;
        }
        
        await saveMapping();
    });
}

function suggestMapping(field, columns) {
    const fieldLower = field.toLowerCase();
    const columnLower = columns.map(c => c.toLowerCase());

    // Exact matches
    for (let i = 0; i < columns.length; i++) {
        if (fieldLower.includes(columnLower[i]) || columnLower[i].includes(fieldLower)) {
            return columns[i];
        }
    }

    // Keyword matches
    const keywords = {
        'Order Number': ['order', 'number'],
        'Order Date': ['date', 'shipped'],
        'Zip': ['zip', 'postal'],
        'Weight (oz)': ['weight', 'oz'],
        'Shipping Carrier': ['carrier'],
        'Shipping Service': ['service'],
        'Package Height': ['height'],
        'Package Width': ['width'],
        'Package Length': ['length'],
        'Zone': ['zone'],
        'Label Cost': ['cost', 'rate', 'shipping']
    };

    if (keywords[field]) {
        for (const keyword of keywords[field]) {
            for (let i = 0; i < columns.length; i++) {
                if (columnLower[i].includes(keyword)) {
                    return columns[i];
                }
            }
        }
    }

    return null;
}

function updateMappingStatus(row, field, isMapped) {
    const statusEl = row.querySelector('.status');
    if (isMapped) {
        statusEl.textContent = 'Confirmed';
        statusEl.className = 'status confirmed';
    } else {
        statusEl.textContent = 'Not mapped';
        statusEl.className = 'status not-mapped';
    }
}

function validateMapping() {
    const requiredFields = [
        'Order Number',
        'Order Date',
        'Zip',
        'Weight (oz)',
        'Shipping Carrier',
        'Shipping Service',
        'Package Height',
        'Package Width',
        'Package Length',
        'Zone'
    ];

    const missing = requiredFields.filter(f => !mappingData[f]);
    
    document.querySelectorAll('.mapping-row').forEach(row => {
        const field = row.dataset.field;
        if (requiredFields.includes(field) && !mappingData[field]) {
            row.classList.add('error');
            row.querySelector('select').classList.add('error');
            const errorMessage = row.querySelector('.error-message');
            if (errorMessage) {
                errorMessage.style.display = 'block';
            }
        } else {
            row.classList.remove('error');
            row.querySelector('select').classList.remove('error');
            const errorMessage = row.querySelector('.error-message');
            if (errorMessage) {
                errorMessage.style.display = 'none';
            }
        }
    });

    document.getElementById('continueBtn2').disabled = missing.length > 0;
    return missing.length === 0;
}

function showLabelCostModal() {
    document.getElementById('labelCostModal').style.display = 'flex';
    document.getElementById('continueAnywayBtn').onclick = async () => {
        document.getElementById('labelCostModal').style.display = 'none';
        await saveMapping();
    };
    document.getElementById('mapLabelCostBtn').onclick = () => {
        document.getElementById('labelCostModal').style.display = 'none';
        // Scroll to Label Cost field
        const labelCostRow = document.querySelector('[data-field="Label Cost"]');
        if (labelCostRow) {
            labelCostRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
            labelCostRow.querySelector('select').focus();
        }
    };
}

async function saveMapping() {
    const merchantName = document.getElementById('merchantName').value;
    const merchantId = document.getElementById('merchantId').value;
    const existingCustomer = document.getElementById('existingCustomer').checked;
    const originZip = document.getElementById('originZip').value;

    try {
        const response = await fetch('/api/mapping', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: jobId,
                merchant_name: merchantName,
                merchant_id: merchantId,
                existing_customer: existingCustomer,
                origin_zip: originZip,
                mapping: mappingData,
                structure: uploadData.detected_structure
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Mapping failed');
        }

        showStep(3);
        loadServiceLevelsStep();
    } catch (error) {
        alert('Error saving mapping: ' + error.message);
    }
}

// Step 3: Service Levels
function loadServiceLevelsStep() {
    const merchantName = document.getElementById('merchantName').value;
    document.getElementById('merchantNameDisplay').textContent = merchantName;

    const serviceList = document.getElementById('serviceList');
    serviceList.innerHTML = '';

    const alwaysOnServices = ['USPS Ground Advantage', 'UPS® Ground'];

    SERVICE_LEVELS.forEach(service => {
        const item = document.createElement('div');
        item.className = 'service-item';

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = `service-${service}`;
        checkbox.value = service;
        if (alwaysOnServices.includes(service)) {
            checkbox.checked = true;
            checkbox.disabled = true;
            if (!selectedServices.includes(service)) {
                selectedServices.push(service);
            }
        }

        checkbox.addEventListener('change', () => {
            if (checkbox.checked) {
                if (!selectedServices.includes(service)) {
                    selectedServices.push(service);
                }
            } else {
                selectedServices = selectedServices.filter(s => s !== service);
            }
            validateServiceLevels();
        });

        const label = document.createElement('label');
        label.htmlFor = `service-${service}`;
        label.textContent = service;

        item.appendChild(checkbox);
        item.appendChild(label);
        serviceList.appendChild(item);
    });

    // Search functionality
    document.getElementById('serviceSearch').addEventListener('input', (e) => {
        const searchTerm = e.target.value.toLowerCase();
        document.querySelectorAll('.service-item').forEach(item => {
            const text = item.textContent.toLowerCase();
            item.style.display = text.includes(searchTerm) ? 'flex' : 'none';
        });
    });

    validateServiceLevels();

    document.getElementById('backBtn3').addEventListener('click', () => showStep(2));
    document.getElementById('continueBtn3').addEventListener('click', async () => {
        await saveServiceLevels();
    });
}

function validateServiceLevels() {
    const hasSelection = selectedServices.length > 0;
    document.getElementById('continueBtn3').disabled = !hasSelection;
}

async function saveServiceLevels() {
    try {
        const response = await fetch('/api/service-levels', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: jobId,
                selected_services: selectedServices
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to save service levels');
        }

        showStep(4);
        await generateRateCard();
    } catch (error) {
        alert('Error saving service levels: ' + error.message);
    }
}

// Step 4: Generating
async function generateRateCard() {
    // Update progress
    document.getElementById('progressStep1').classList.add('completed');
    
    try {
        const response = await fetch('/api/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: jobId })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Generation failed');
        }

        // Simulate progress
        setTimeout(() => {
            document.getElementById('progressStep2').classList.add('completed');
        }, 500);

        setTimeout(() => {
            document.getElementById('progressStep3').classList.add('completed');
        }, 1000);

        setTimeout(() => {
            showStep(5);
            setupDownloadLinks();
        }, 1500);
    } catch (error) {
        alert('Error generating rate card: ' + error.message);
    }
}

// Step 5: Complete
function setupDownloadLinks() {
    const merchantName = document.getElementById('merchantName').value;
    
    document.getElementById('downloadRateCard').href = `/download/${jobId}/rate-card`;
    document.getElementById('downloadRawInvoice').href = `/download/${jobId}/raw-invoice`;
    document.getElementById('downloadNormalized').href = `/download/${jobId}/normalized`;
    document.getElementById('sharepointLink').href = `https://sharepoint.example.com/view?merchant=${encodeURIComponent(merchantName)}`;

    document.getElementById('toggleSupportingFiles').addEventListener('click', () => {
        const list = document.getElementById('supportingFilesList');
        list.style.display = list.style.display === 'none' ? 'block' : 'none';
    });

    document.getElementById('backBtn5').addEventListener('click', () => showStep(4));
    document.getElementById('startOverBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to start over?')) {
            location.reload();
        }
    });
    document.getElementById('finishBtn').addEventListener('click', () => {
        alert('Rate card generation complete!');
        location.reload();
    });
}
