// === State ===
let bankFile = null;
let bankColumns = [];
let lmsFileList = [];
let reconResult = null;
const REQUIRED_FIELDS = ["TxnID", "Amount", "Date", "Description"];

// === Page Navigation ===
function showPage(page) {
    document.querySelectorAll('[id^="page-"]').forEach(p => p.classList.add('hidden'));
    document.getElementById(`page-${page}`).classList.remove('hidden');
    document.querySelectorAll('.page-btn').forEach(b => { b.classList.remove('active'); });
    document.getElementById(`nav-${page}`).classList.add('active');
    if (page === 'history') loadHistory();
}

// === Bank File Upload ===
async function handleBankUpload(input) {
    const file = input.files[0];
    if (!file) return;
    bankFile = file;
    document.getElementById('bankLabel').textContent = file.name;

    const form = new FormData();
    form.append('bank_file', file);

    try {
        const res = await fetch('/api/preview', { method: 'POST', body: form });
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        bankColumns = data.columns;
        document.getElementById('bankInfo').textContent = `Loaded ${data.row_count.toLocaleString()} rows, ${data.col_count} columns`;
        document.getElementById('bankPreviewTable').innerHTML = buildTable(data.preview);
        document.getElementById('bankPreview').classList.remove('hidden');

        renderMapping(data.columns);
        updateRunButton();
    } catch (e) {
        document.getElementById('bankInfo').textContent = `Error: ${e.message}`;
        document.getElementById('bankInfo').className = 'text-sm text-red-600 font-medium mb-3';
        document.getElementById('bankPreview').classList.remove('hidden');
    }
}

// === LMS Files Upload ===
function handleLmsUpload(input) {
    lmsFileList = Array.from(input.files);
    const info = document.getElementById('lmsInfo');
    if (lmsFileList.length === 0) {
        info.innerHTML = '';
        return;
    }
    info.innerHTML = `<p class="text-sm text-green-600 font-medium">${lmsFileList.length} file(s) selected</p>` +
        lmsFileList.map(f => `<p class="text-xs text-gray-500 ml-2">${f.name} (${(f.size / 1024).toFixed(1)} KB)</p>`).join('');
    document.getElementById('lmsLabel').textContent = `${lmsFileList.length} file(s) selected`;
    updateRunButton();
}

// === Column Mapping ===
function renderMapping(columns) {
    const container = document.getElementById('mappingFields');
    container.innerHTML = '';
    REQUIRED_FIELDS.forEach(field => {
        const defaultIdx = columns.findIndex(c => c.toLowerCase().includes(field.toLowerCase()));
        const div = document.createElement('div');
        div.innerHTML = `
            <label class="block text-sm font-medium text-gray-700 mb-1">${field}</label>
            <select id="map_${field}" onchange="validateMapping()" class="w-full border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                ${columns.map((c, i) => `<option value="${c}" ${i === (defaultIdx >= 0 ? defaultIdx : 0) ? 'selected' : ''}>${c}</option>`).join('')}
            </select>`;
        container.appendChild(div);
    });
    document.getElementById('mappingSection').classList.remove('hidden');
    validateMapping();
}

function getColumnMap() {
    const map = {};
    REQUIRED_FIELDS.forEach(f => {
        const sel = document.getElementById(`map_${f}`);
        if (sel) map[f] = sel.value;
    });
    return map;
}

function validateMapping() {
    const map = getColumnMap();
    const values = Object.values(map);
    const hasDupes = new Set(values).size !== values.length;
    const err = document.getElementById('mappingError');
    if (hasDupes) {
        err.textContent = 'Each field must be mapped to a different column.';
        err.classList.remove('hidden');
    } else {
        err.classList.add('hidden');
    }
    updateRunButton();
    return !hasDupes;
}

function updateRunButton() {
    const map = getColumnMap();
    const values = Object.values(map);
    const valid = bankFile && lmsFileList.length > 0 && values.length === REQUIRED_FIELDS.length && new Set(values).size === values.length;
    document.getElementById('runBtn').disabled = !valid;
    const status = document.getElementById('uploadStatus');
    const missing = [];
    if (!bankFile) missing.push('bank statement');
    if (lmsFileList.length === 0) missing.push('LMS file(s)');
    status.textContent = missing.length ? `Please upload: ${missing.join(', ')}` : '';
}

// === Run Reconciliation ===
async function runReconciliation() {
    if (!validateMapping()) return;
    const btn = document.getElementById('runBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="loader"></span> Running...';

    const form = new FormData();
    form.append('bank_file', bankFile);
    lmsFileList.forEach(f => form.append('lms_files', f));
    form.append('column_map', JSON.stringify(getColumnMap()));

    try {
        const res = await fetch('/api/reconcile', { method: 'POST', body: form });
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        reconResult = data;
        renderResults(data);
        document.getElementById('nav-results').disabled = false;
        showPage('results');
    } catch (e) {
        alert(`Reconciliation failed: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Run Reconciliation';
    }
}

// === Render Results ===
function renderResults(data) {
    const s = data.summary;

    // Top metrics
    const metrics = [
        { label: 'Total Bank Txns', value: fmt(s['Total Bank Transactions']) },
        { label: 'Matched', value: fmt(s['Matched']), delta: `${s['Match Rate (%)']}%` },
        { label: 'Mismatches', value: fmt(s['Amount Mismatches']) },
        { label: 'Bank Only', value: fmt(s['Bank Only']) },
        { label: 'LMS Only', value: fmt(s['LMS Only']) },
        { label: 'Duplicates', value: fmt(s['Bank Duplicates']) },
    ];
    document.getElementById('metricsRow').innerHTML = metrics.map(m => metricCard(m.label, m.value, m.delta)).join('');

    // Amount metrics
    const amounts = [
        { label: 'Matched Amount', value: fmtAmt(s['Matched Amount (Bank)']) },
        { label: 'Mismatch Amount', value: fmtAmt(s['Mismatch Amount (Bank)']) },
        { label: 'Bank Only Amount', value: fmtAmt(s['Bank Only Amount']) },
        { label: 'LMS Only Amount', value: fmtAmt(s['LMS Only Amount']) },
    ];
    document.getElementById('amountMetrics').innerHTML = amounts.map(m => metricCard(m.label, m.value)).join('');

    // Brand summary
    document.getElementById('brandSummaryTable').innerHTML = data.brand_summary.length
        ? buildTable(data.brand_summary) : '<p class="text-gray-500">No brand data</p>';

    // LMS dupe warning
    const warn = document.getElementById('lmsDupeWarning');
    if (data.lms_duplicate_count > 0) {
        warn.textContent = `Found ${data.lms_duplicate_count.toLocaleString()} duplicate TxnIDs in LMS files.`;
        warn.classList.remove('hidden');
    } else {
        warn.classList.add('hidden');
    }

    // Detail tabs
    const tabs = [
        { key: 'matched', label: 'Matched', count: data.matched.length },
        { key: 'amount_mismatch', label: 'Amount Mismatch', count: data.amount_mismatch.length },
        { key: 'bank_only', label: 'Bank Only', count: data.bank_only.length },
        { key: 'lms_only', label: 'LMS Only', count: data.lms_only.length },
        { key: 'bank_duplicates', label: 'Duplicates', count: data.bank_duplicates.length },
    ];

    document.getElementById('detailTabs').innerHTML = tabs.map((t, i) =>
        `<button class="tab-btn px-4 py-2 border-b-2 text-sm font-medium transition ${i === 0 ? 'active border-blue-500' : 'border-transparent text-gray-500 hover:text-gray-700'}" onclick="switchTab('${t.key}', this)">${t.label} (${t.count.toLocaleString()})</button>`
    ).join('');

    switchTab('matched', document.querySelector('.tab-btn'));
}

function switchTab(key, btn) {
    document.querySelectorAll('.tab-btn').forEach(b => { b.classList.remove('active', 'border-blue-500'); b.classList.add('border-transparent', 'text-gray-500'); });
    btn.classList.add('active', 'border-blue-500');
    btn.classList.remove('border-transparent', 'text-gray-500');

    const rows = reconResult[key];
    document.getElementById('detailContent').innerHTML = rows.length
        ? `<p class="text-sm text-gray-500 mb-2">${rows.length.toLocaleString()} rows</p><div class="table-wrap">${buildTable(rows)}</div>`
        : '<p class="text-gray-500 py-4">No records found.</p>';
}

// === Download Report ===
async function downloadReport() {
    const form = new FormData();
    form.append('bank_file', bankFile);
    lmsFileList.forEach(f => form.append('lms_files', f));
    form.append('column_map', JSON.stringify(getColumnMap()));

    try {
        const res = await fetch('/api/report', { method: 'POST', body: form });
        if (!res.ok) throw new Error('Download failed');
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'reconciliation_report.xlsx';
        a.click();
        URL.revokeObjectURL(url);
    } catch (e) {
        alert(`Download failed: ${e.message}`);
    }
}

// === History ===
async function loadHistory() {
    const el = document.getElementById('historyContent');
    el.innerHTML = '<p class="text-gray-500">Loading...</p>';
    try {
        const res = await fetch('/api/history');
        const data = await res.json();
        if (data.runs && data.runs.length > 0) {
            el.innerHTML = `<div class="table-wrap">${buildTable(data.runs)}</div>`;
        } else {
            el.innerHTML = `<p class="text-gray-500">${data.message || 'No reconciliation runs found yet.'}</p>`;
        }
    } catch (e) {
        el.innerHTML = `<p class="text-red-500">Failed to load history: ${e.message}</p>`;
    }
}

// === Helpers ===
function fmt(n) { return (n || 0).toLocaleString(); }
function fmtAmt(n) { return (n || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }

function metricCard(label, value, delta) {
    return `<div class="bg-white rounded-xl shadow-sm border p-4">
        <p class="text-xs text-gray-500 uppercase tracking-wide">${label}</p>
        <p class="text-2xl font-bold text-gray-800 mt-1">${value}</p>
        ${delta ? `<p class="text-sm text-green-600 mt-1">${delta}</p>` : ''}
    </div>`;
}

function buildTable(rows) {
    if (!rows || rows.length === 0) return '<p class="text-gray-500">No data</p>';
    const keys = Object.keys(rows[0]);
    const header = '<tr>' + keys.map(k => `<th>${k}</th>`).join('') + '</tr>';
    const body = rows.slice(0, 500).map(row =>
        '<tr>' + keys.map(k => `<td>${row[k] ?? ''}</td>`).join('') + '</tr>'
    ).join('');
    const note = rows.length > 500 ? `<p class="text-xs text-gray-400 mt-1">Showing first 500 of ${rows.length.toLocaleString()} rows</p>` : '';
    return `<table><thead>${header}</thead><tbody>${body}</tbody></table>${note}`;
}
