import { Counter, Rate } from 'k6/metrics';
import http from 'k6/http';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { sleep, check } from 'k6';

// --- Metrics ---
// Domain-specific metrics for device load testing
const usageReported = new Counter('usage_reported'); // Successful usage reports sent
const usageReportFailed = new Counter('usage_report_failed'); // Failed usage reports
const usageReportRate = new Rate('usage_report_rate'); // Rate of usage reports (successful)
const devicePaused = new Counter('device_paused'); // Times device was paused (STOP/PAUSE command)
const deviceConnected = new Counter('device_connected'); // Successful device connections
const deviceConnectionFailed = new Counter('device_connection_failed'); // Failed device connections

// Initialize metrics to 0 to ensure they appear in results even if never used
// Note: k6 only shows metrics that have been used, but initializing helps with visibility

const API_BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8080';
const API_DEVICES_BATCH_ENDPOINT = __ENV.API_DEVICES_BATCH_ENDPOINT || '/devices/batch';
const HTTPDEVICE_BASE_URL = __ENV.HTTPDEVICE_BASE_URL || 'http://localhost:3002';
const USAGE_REPORT_INTERVAL = parseInt(__ENV.USAGE_REPORT_INTERVAL || '1'); // seconds between reports
const UNIT_PRICE_MSAT = parseInt(__ENV.UNIT_PRICE_MSAT || '100');
const AUTHORIZE_REQUEST_MSAT = parseInt(__ENV.AUTHORIZE_REQUEST_MSAT || '10000');

const WARMUP = '30s';
const MEASURE = '60s';

// Define load test stages
const loadTestStages = [
  { duration: WARMUP, target: 25 },
  { duration: MEASURE, target: 25 },   // warmup
  { duration: WARMUP, target: 50 },
  { duration: MEASURE, target: 50 },
  { duration: WARMUP, target: 0 },
  { duration: WARMUP, target: 75 },
  { duration: MEASURE, target: 75 },
  { duration: WARMUP, target: 100 },
  { duration: MEASURE, target: 100 },
  { duration: WARMUP, target: 125 },
  { duration: MEASURE, target: 125 },
  { duration: WARMUP, target: 150 },
  { duration: MEASURE, target: 150 },
  { duration: WARMUP, target: 175 },
  { duration: MEASURE, target: 175 },
  { duration: WARMUP, target: 200 },
  { duration: MEASURE, target: 200 },
];

// Calculate maximum VU count from stages (for setup - register all devices that will be used)
const maxVUsFromStages = Math.max(...loadTestStages.map(stage => stage.target || 0));
const VUsCount = maxVUsFromStages;

// --- Configuration ---
export const options = {
  setupTimeout: '5m', // Allow up to 5 minutes for setup (device batch registration)
  scenarios: {
    devices: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: loadTestStages,
      gracefulRampDown: '2m',
      tags: { type: 'loadtest' },
    },
  },
  thresholds: {
    'http_req_duration{type:loadtest}': ['p(99)<300', 'p(99.9)<500', 'max<1000'],
  },
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)', 'max'],
};

// --- Helpers ---
function generateDeviceID(vuID) {
  // Match the pattern used in setup: k6_device_{id} with 6-digit padding
  return `k6_device_${String(vuID).padStart(6, '0')}`;
}

function generateID() {
  return randomString(16, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789');
}

function getISOTimestamp() {
  return new Date().toISOString();
}


// --- Setup ---
export function setup() {
  const vuSource = __ENV.VUS ? 'VUS environment variable' : `maximum from stages (${maxVUsFromStages})`;
  console.log(`Starting load test setup: pre-registering ${VUsCount} devices in batch (${vuSource})...`);

  // Generate device IDs
  const deviceIDs = [];
  for (let id = 1; id <= VUsCount; id++) {
    const deviceID = `k6_device_${String(id).padStart(6, '0')}`;
    deviceIDs.push(deviceID);
  }

  console.log(`Generated ${deviceIDs.length} device IDs (range: k6_device_000001 to k6_device_${String(VUsCount).padStart(6, '0')})`);

  // Register all devices using batch endpoint
  const batchPayload = JSON.stringify({
    device_id_pattern: 'k6_device_{id}',
    device_secret_pattern: 'k6_device_{id}_password',
    id_start: 1,
    id_end: VUsCount,
    id_padding: 6,
    measurement_unit: 'kWh',
    unit_price_msat: UNIT_PRICE_MSAT,
    reporting_strategy: 'interval',
    reporting_interval: USAGE_REPORT_INTERVAL,
    heartbeat_interval: 60,
    authorize_request_msat: AUTHORIZE_REQUEST_MSAT,
    timestamp: getISOTimestamp(),
  });

  console.log(`Registering ${VUsCount} devices via batch endpoint...`);
  const batchRes = http.post(
    `${API_BASE_URL}${API_DEVICES_BATCH_ENDPOINT}`,
    batchPayload,
    { headers: { 'Content-Type': 'application/json' } }
  );

  let registered = 0;
  if (batchRes.status === 204) {
    console.log(`Batch already exists (204 No Content) - all ${VUsCount} devices are already registered`);
    registered = VUsCount;
  } else if (batchRes.status === 201) {
    const response = JSON.parse(batchRes.body);
    console.log(`Batch registration successful: ${response.devices_created} devices created (range: ${response.id_range})`);
    registered = response.devices_created;
  } else {
    console.error(`Failed to register device batch: ${batchRes.status} - ${batchRes.body}`);
    return {
      deviceIDs: [],
      registered: 0,
    };
  }

  const setupEndTime = new Date().toISOString();
  console.log(`[${setupEndTime}] Setup complete: ${registered}/${VUsCount} devices registered`);
  return {
    deviceIDs,
    registered,
  };
}

// --- Main VU Function ---
export default function () {
  const vuID = __VU;
  const deviceID = generateDeviceID(vuID);

  // Connect device on first iteration
  if (__ITER === 0) {
    const firstIterationTime = new Date().toISOString();
    console.log(`[${firstIterationTime}] VU ${vuID} (${deviceID}) - First iteration started, connecting...`);

    const deviceSecret = `${deviceID}_password`;
    const connectPayload = JSON.stringify({
      secret: deviceSecret,
    });

    const connectRes = http.post(
      `${HTTPDEVICE_BASE_URL}/devices/${deviceID}/connect`,
      connectPayload,
      {
        headers: { 'Content-Type': 'application/json' },
        timeout: '120s', // Allow time for invoice + authorization
      }
    );

    if (check(connectRes, { 'Device connected': (r) => r.status === 200 })) {
      deviceConnected.add(1);
      console.log(`VU ${vuID} successfully connected device ${deviceID}`);
    } else {
      deviceConnectionFailed.add(1);
      console.error(`VU ${vuID} failed to connect device ${deviceID}: ${connectRes.status} - ${connectRes.body}`);
    }
  }

  // k6 calls this function in a loop - each call sends one usage report
  // The httpdevice handles all the MQTT logic, authorization maintenance, etc.

  // Generate a random measurement between 0.1 and 1.0 kWh
  const measure = 0.1 + Math.random() * 0.9;
  const usagePayload = JSON.stringify({
    deviceId: deviceID,
    reportId: generateID(),
    strategy: 1,
    measure: measure,
    unit: 'kWh',
    timestamp: getISOTimestamp(),
  });

  // Send usage report via httpdevice
  const usageRes = http.post(
    `${HTTPDEVICE_BASE_URL}/devices/${deviceID}/usage`,
    usagePayload,
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (usageRes.status === 200) {
    usageReported.add(1);
    usageReportRate.add(1);
    // Ensure device_paused metric is always visible (initialize to 0 if not paused)
    console.log(`[VU ${vuID}] Usage report sent (${JSON.parse(usagePayload).reportId}): ${measure.toFixed(4)} kWh`);
    devicePaused.add(0);
  } else if (usageRes.status === 423) {
    // 423 = Locked/Reporting disabled (STOP/PAUSE command received)
    // Device is paused, not failed - k6 will continue calling this function
    devicePaused.add(1);
  } else {
    usageReportFailed.add(1);
    // Ensure device_paused metric is always visible (initialize to 0 if not paused)
    devicePaused.add(0);
    console.error(`[VU ${vuID}] Usage report failed: ${usageRes.status} - ${usageRes.body}`);
  }

  sleep(1); // Sleep for 1 second for each usage report

  // Sleep for a random interval between 0.1 and 1.0 seconds
  // This creates realistic, desynchronized load patterns
  // const sleepDuration = 0.1 + Math.random() * 0.5; // Random between 0.1 and 1.0 seconds
  // sleep(sleepDuration);
}

// --- Teardown ---
export function teardown(data) {
  console.log("Disconnecting all devices...");

  let deviceIDs = data?.deviceIDs || [];
  
  // Fallback: generate device IDs if not in data
  if (deviceIDs.length === 0) {
    console.log("No device IDs in data, generating device IDs...");
    for (let id = 1; id <= VUsCount; id++) {
      const deviceID = `k6_device_${String(id).padStart(6, '0')}`;
      deviceIDs.push(deviceID);
    }
  }

  let totalDisconnected = 0;
  let totalFailed = 0;

  // Disconnect devices in chunks for better performance
  const chunkSize = 10; // Disconnect 10 devices at a time
  for (let i = 0; i < deviceIDs.length; i += chunkSize) {
    const chunk = deviceIDs.slice(i, i + chunkSize);
    const batchPayload = JSON.stringify({ deviceIds: chunk });

    const batchRes = http.post(
      `${HTTPDEVICE_BASE_URL}/devices/batch/disconnect`,
      batchPayload,
      {
        headers: { 'Content-Type': 'application/json' },
        timeout: '30s',
      }
    );

    if (batchRes.status === 200) {
      const result = JSON.parse(batchRes.body);
      totalDisconnected += result.disconnected;
      totalFailed += result.failed;

      // Log progress
      const progress = Math.min(i + chunkSize, deviceIDs.length);
      console.log(`Disconnected batch: ${result.disconnected}/${chunk.length} (total: ${totalDisconnected}/${progress})`);

      // Log any failures
      if (result.failed > 0) {
        const failedDevices = result.results.filter(r => !r.success);
        failedDevices.forEach(f => {
          console.error(`Failed to disconnect ${f.deviceId}: ${f.error || 'unknown error'}`);
        });
      }
    } else {
      // Entire batch failed
      totalFailed += chunk.length;
      console.error(`Batch disconnect failed: ${batchRes.status} - ${batchRes.body}`);
    }
  }

  console.log(`Teardown complete: ${totalDisconnected} disconnected, ${totalFailed} failed`);
  console.log("Load test finished.");
}
