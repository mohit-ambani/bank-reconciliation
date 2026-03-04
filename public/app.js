// === State ===
let bankFile = null;
let bankColumns = [];
let bankData = [];       // parsed rows from bank file
let lmsFileList = [];
let lmsData = [];        // parsed rows from all LMS files
let reconResult = null;
const REQUIRED_FIELDS = ["TxnID", "Amount", "Date", "Description"];

// Extra bank columns to always include (status for cross-match)
const BANK_EXTRA_COLS = ["status"];
// LMS columns we always need
const LMS_KNOWN_COLS = ["TxnID", "TransID", "transid", "Amount", "amount", "Date", "CreatedOn", "createdon", "created_on", "created_at", "TransStatus", "transstatus"];

// === File Parsing (client-side via SheetJS) ===
function parseFile(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const wb = XLSX.read(e.target.result, { type: 'array', cellDates: true });
                const sheet = wb.Sheets[wb.SheetNames[0]];
                const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
                resolve(rows);
            } catch (err) {
                reject(err);
            }
        };
        reader.onerror = () => reject(new Error('Failed to read file'));
        reader.readAsArrayBuffer(file);
    });
}

// === Gzip Compression ===
async function gzipData(jsonString) {
    const stream = new Blob([jsonString]).stream().pipeThrough(new CompressionStream('gzip'));
    return new Response(stream).arrayBuffer();
}

// === Trim Data for Sending ===
function trimBankData(rows, columnMap) {
    // Keep only mapped columns + status
    const keepCols = new Set([...Object.values(columnMap), ...BANK_EXTRA_COLS]);
    return rows.map(row => {
        const slim = {};
        for (const k of keepCols) {
            if (k in row) slim[k] = row[k];
        }
        return slim;
    });
}

function trimLmsData(rows) {
    // Keep only columns that match known LMS column names (case-insensitive)
    if (rows.length === 0) return rows;
    const allKeys = Object.keys(rows[0]);
    const lmsLower = new Set(LMS_KNOWN_COLS.map(c => c.toLowerCase()));
    const keepCols = allKeys.filter(k => lmsLower.has(k.toLowerCase()) || k === '_sourceFile');
    return rows.map(row => {
        const slim = {};
        for (const k of keepCols) {
            slim[k] = row[k];
        }
        return slim;
    });
}

async function sendCompressed(url, payload) {
    const jsonStr = JSON.stringify(payload);
    const compressed = await gzipData(jsonStr);
    return fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip',
        },
        body: compressed,
    });
}

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

    try {
        const rows = await parseFile(file);
        bankData = rows;
        bankColumns = rows.length > 0 ? Object.keys(rows[0]) : [];

        document.getElementById('bankInfo').textContent = `Loaded ${rows.length.toLocaleString()} rows, ${bankColumns.length} columns`;
        document.getElementById('bankPreviewTable').innerHTML = buildTable(rows.slice(0, 5));
        document.getElementById('bankPreview').classList.remove('hidden');

        renderMapping(bankColumns);
        updateRunButton();
    } catch (e) {
        document.getElementById('bankInfo').textContent = `Error: ${e.message}`;
        document.getElementById('bankInfo').className = 'text-sm text-red-600 font-medium mb-3';
        document.getElementById('bankPreview').classList.remove('hidden');
    }
}

// === LMS Files Upload ===
async function handleLmsUpload(input) {
    lmsFileList = Array.from(input.files);
    const info = document.getElementById('lmsInfo');
    if (lmsFileList.length === 0) {
        info.innerHTML = '';
        lmsData = [];
        return;
    }
    info.innerHTML = `<p class="text-sm text-blue-600 font-medium">Parsing ${lmsFileList.length} file(s)...</p>`;

    try {
        lmsData = [];
        for (const f of lmsFileList) {
            const rows = await parseFile(f);
            rows.forEach(r => r._sourceFile = f.name);
            lmsData = lmsData.concat(rows);
        }
        info.innerHTML = `<p class="text-sm text-green-600 font-medium">${lmsFileList.length} file(s) — ${lmsData.length.toLocaleString()} total rows</p>` +
            lmsFileList.map(f => `<p class="text-xs text-gray-500 ml-2">${f.name} (${(f.size / 1024).toFixed(1)} KB)</p>`).join('');
        document.getElementById('lmsLabel').textContent = `${lmsFileList.length} file(s) selected`;
        updateRunButton();
    } catch (e) {
        info.innerHTML = `<p class="text-sm text-red-600 font-medium">Error parsing LMS files: ${e.message}</p>`;
        lmsData = [];
    }
}

// === Column Mapping ===
const COLUMN_ALIASES = {
    "TxnID": ["payouts.reference_id", "reference_id", "transid", "trans_id", "txnid", "utr", "transaction_id"],
    "Amount": ["amount", "debit", "credit", "total"],
    "Date": ["created_at", "processed_at", "date", "txn_date", "transaction_date", "createdon"],
    "Description": ["status_description", "description", "narration", "remarks", "purpose"],
};

function findBestColumn(columns, field) {
    let idx = columns.findIndex(c => c.toLowerCase() === field.toLowerCase());
    if (idx >= 0) return idx;
    const aliases = COLUMN_ALIASES[field] || [];
    for (const alias of aliases) {
        idx = columns.findIndex(c => c.toLowerCase() === alias.toLowerCase());
        if (idx >= 0) return idx;
    }
    idx = columns.findIndex(c => c.toLowerCase().includes(field.toLowerCase()));
    if (idx >= 0) return idx;
    for (const alias of aliases) {
        idx = columns.findIndex(c => c.toLowerCase().includes(alias.toLowerCase()));
        if (idx >= 0) return idx;
    }
    return -1;
}

function renderMapping(columns) {
    const container = document.getElementById('mappingFields');
    container.innerHTML = '';
    REQUIRED_FIELDS.forEach(field => {
        const defaultIdx = findBestColumn(columns, field);
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
    const valid = bankData.length > 0 && lmsData.length > 0 && values.length === REQUIRED_FIELDS.length && new Set(values).size === values.length;
    document.getElementById('runBtn').disabled = !valid;
    const status = document.getElementById('uploadStatus');
    const missing = [];
    if (bankData.length === 0) missing.push('bank statement');
    if (lmsData.length === 0) missing.push('LMS file(s)');
    status.textContent = missing.length ? `Please upload: ${missing.join(', ')}` : '';
}

// === Run Reconciliation ===
async function runReconciliation() {
    if (!validateMapping()) return;
    const btn = document.getElementById('runBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="loader"></span> Running...';

    const columnMap = getColumnMap();
    const payload = {
        bank_data: trimBankData(bankData, columnMap),
        lms_data: trimLmsData(lmsData),
        column_map: columnMap,
    };

    try {
        const res = await sendCompressed('/api/reconcile', payload);
        const text = await res.text();
        let data;
        try { data = JSON.parse(text); } catch { throw new Error(res.ok ? text.slice(0, 300) : `Server error ${res.status}: ${text.slice(0, 300)}`); }
        if (data.error) throw new Error(data.trace || data.error);

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

    const metrics = [
        { label: 'Total Bank Txns', value: fmt(s['Total Bank Transactions']) },
        { label: 'Matched', value: fmt(s['Matched']), delta: `${s['Match Rate (%)']}%` },
        { label: 'Mismatches', value: fmt(s['Amount Mismatches']) },
        { label: 'Bank Only', value: fmt(s['Bank Only']) },
        { label: 'LMS Only', value: fmt(s['LMS Only']) },
        { label: 'Duplicates', value: fmt(s['Bank Duplicates']) },
    ];
    document.getElementById('metricsRow').innerHTML = metrics.map(m => metricCard(m.label, m.value, m.delta)).join('');

    const amounts = [
        { label: 'Matched Amount', value: fmtAmt(s['Matched Amount (Bank)']) },
        { label: 'Mismatch Amount', value: fmtAmt(s['Mismatch Amount (Bank)']) },
        { label: 'Bank Only Amount', value: fmtAmt(s['Bank Only Amount']) },
        { label: 'LMS Only Amount', value: fmtAmt(s['LMS Only Amount']) },
    ];
    document.getElementById('amountMetrics').innerHTML = amounts.map(m => metricCard(m.label, m.value)).join('');

    document.getElementById('brandSummaryTable').innerHTML = data.brand_summary.length
        ? buildTable(data.brand_summary) : '<p class="text-gray-500">No brand data</p>';

    document.getElementById('statusCrossMatchTable').innerHTML = data.status_cross_match && data.status_cross_match.length
        ? buildTable(data.status_cross_match) : '<p class="text-gray-500">No status cross-match data</p>';

    const warn = document.getElementById('lmsDupeWarning');
    if (data.lms_duplicate_count > 0) {
        warn.textContent = `Found ${data.lms_duplicate_count.toLocaleString()} duplicate TxnIDs in LMS files.`;
        warn.classList.remove('hidden');
    } else {
        warn.classList.add('hidden');
    }

    const tabs = [
        { key: 'matched', label: 'Matched', count: data.matched_total || data.matched.length },
        { key: 'amount_mismatch', label: 'Amount Mismatch', count: data.amount_mismatch_total || data.amount_mismatch.length },
        { key: 'bank_only', label: 'Bank Only', count: data.bank_only_total || data.bank_only.length },
        { key: 'lms_only', label: 'LMS Only', count: data.lms_only_total || data.lms_only.length },
        { key: 'bank_duplicates', label: 'Duplicates', count: data.bank_duplicates_total || data.bank_duplicates.length },
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
    const total = reconResult[key + '_total'] || rows.length;
    const capped = total > rows.length ? `<p class="text-sm text-amber-600 mb-2">Showing ${rows.length.toLocaleString()} of ${total.toLocaleString()} rows (download Excel for full data)</p>` : `<p class="text-sm text-gray-500 mb-2">${rows.length.toLocaleString()} rows</p>`;
    document.getElementById('detailContent').innerHTML = rows.length
        ? `${capped}<div class="table-wrap">${buildTable(rows)}</div>`
        : '<p class="text-gray-500 py-4">No records found.</p>';
}

// === Download Report ===
async function downloadReport() {
    const btn = event.target;
    btn.disabled = true;
    btn.textContent = 'Generating...';

    const columnMap = getColumnMap();
    const payload = {
        bank_data: trimBankData(bankData, columnMap),
        lms_data: trimLmsData(lmsData),
        column_map: columnMap,
    };

    try {
        const res = await sendCompressed('/api/report', payload);
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text.slice(0, 300));
        }
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'reconciliation_report.xlsx';
        a.click();
        URL.revokeObjectURL(url);
    } catch (e) {
        alert(`Download failed: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Download Full Excel Report';
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
