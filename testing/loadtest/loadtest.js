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

const LEVEL_VUS = 25;
const WARMUP = '60s';
const MEASURE = '120s';
const IDLE = '60s';
const TEARDOWN = '60s'

// Define load test stages
const loadTestStages = [
  { duration: IDLE, target: 0 },
  { duration: WARMUP, target: LEVEL_VUS },
  { duration: MEASURE, target: LEVEL_VUS },   // warmup
  { duration: WARMUP, target: LEVEL_VUS * 2 },
  { duration: MEASURE, target: LEVEL_VUS * 2 },
  { duration: WARMUP, target: LEVEL_VUS * 3 },
  { duration: MEASURE, target: LEVEL_VUS * 3 },
  { duration: WARMUP, target: LEVEL_VUS * 4 },
  { duration: MEASURE, target: LEVEL_VUS * 4 },
  { duration: WARMUP, target: LEVEL_VUS * 5 },
  { duration: MEASURE, target: LEVEL_VUS * 5 },
  { duration: WARMUP, target: LEVEL_VUS * 6 },
  { duration: MEASURE, target: LEVEL_VUS * 6 },
  { duration: TEARDOWN, target: 0 },
];

// Calculate maximum VU count from stages (for setup - register all devices that will be used)
const maxVUsFromStages = Math.max(...loadTestStages.map(stage => stage.target || 0));
const VUsCount = maxVUsFromStages;

// --- Configuration ---
export const options = {
  setupTimeout: '10m', // Allow up to 10 minutes for setup (device batch registration + connection)
  scenarios: {
    load_usage: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: loadTestStages,
      gracefulRampDown: '2m',
      exec: 'load_usage',
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
    // Device service republishes retained MQTT config for this batch on 204 (so /devices/<id>/config exists
    // again after broker restart, etc.). MQTT Explorer: subscribe to /devices/k6_device_000001/config, not plain "config".
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
      connected: 0,
    };
  }

  // Connect devices in batches
  const CONNECT_BATCH_SIZE = parseInt(__ENV.CONNECT_BATCH_SIZE || LEVEL_VUS); // Devices per batch
  const CONNECT_BATCH_SLEEP = parseInt(__ENV.CONNECT_BATCH_SLEEP || '5'); // Seconds to sleep between batches
  const CONNECT_TIMEOUT = __ENV.CONNECT_TIMEOUT || '120s'; // Timeout for each connect request

  console.log(`Connecting ${registered} devices in batches of ${CONNECT_BATCH_SIZE} (sleep ${CONNECT_BATCH_SLEEP}s between batches)...`);
  
  let connected = 0;
  let failed = 0;
  
  for (let i = 0; i < deviceIDs.length; i += CONNECT_BATCH_SIZE) {
    const batch = deviceIDs.slice(i, i + CONNECT_BATCH_SIZE);
    const batchNum = Math.floor(i / CONNECT_BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(deviceIDs.length / CONNECT_BATCH_SIZE);
    
    console.log(`Connecting batch ${batchNum}/${totalBatches} (${batch.length} devices)...`);
    
    // Connect devices sequentially within the batch (k6 http.post is synchronous)
    let batchConnected = 0;
    let batchFailed = 0;
    
    for (const deviceID of batch) {
      const deviceSecret = `${deviceID}_password`;
      const connectPayload = JSON.stringify({
        secret: deviceSecret,
      });
      
      const connectRes = http.post(
        `${HTTPDEVICE_BASE_URL}/devices/${deviceID}/connect`,
        connectPayload,
        {
          headers: { 'Content-Type': 'application/json' },
          timeout: CONNECT_TIMEOUT,
        }
      );
      
      if (connectRes.status === 200) {
        batchConnected++;
        deviceConnected.add(1);
      } else {
        batchFailed++;
        deviceConnectionFailed.add(1);
        console.error(`Failed to connect device ${deviceID}: ${connectRes.status} - ${connectRes.body}`);
      }
    }
    
    connected += batchConnected;
    failed += batchFailed;
    
    console.log(`Batch ${batchNum}/${totalBatches} complete: ${batchConnected} connected, ${batchFailed} failed (total: ${connected}/${registered})`);
    
    // Sleep between batches (except after the last batch)
    if (i + CONNECT_BATCH_SIZE < deviceIDs.length) {
      console.log(`Sleeping ${CONNECT_BATCH_SLEEP}s before next batch...`);
      sleep(CONNECT_BATCH_SLEEP);
    }
  }

  const setupEndTime = new Date().toISOString();
  console.log(`[${setupEndTime}] Setup complete: ${registered}/${VUsCount} devices registered, ${connected}/${registered} devices connected, ${failed} failed`);
  
  return {
    deviceIDs,
    registered,
    connected,
    failed,
  };
}

// --- Load Usage Scenario ---
export function load_usage() {
  const vuID = __VU;
  const deviceID = generateDeviceID(vuID);

  // k6 calls this function in a loop - each call sends one usage report
  // The httpdevices service handles all the MQTT logic, authorization maintenance, etc.
  // Devices are assumed to be already connected from setup phase

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

  // Send usage report via httpdevices service
  const usageRes = http.post(
    `${HTTPDEVICE_BASE_URL}/devices/${deviceID}/usage`,
    usagePayload,
    { 
      headers: { 'Content-Type': 'application/json' },
      tags: { type: 'loadtest' },
    }
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

// --- Main VU Function (default - not used when exec is specified) ---
export default function () {
  // This function is not used when scenarios specify exec
  // It's kept for compatibility
  load_usage();
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
