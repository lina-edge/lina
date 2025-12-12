import { Counter, Rate } from 'k6/metrics';
import http from 'k6/http';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { sleep } from 'k6';

// --- Metrics ---
// Domain-specific metrics for device load testing
const usageReported = new Counter('usage_reported'); // Successful usage reports sent
const usageReportFailed = new Counter('usage_report_failed'); // Failed usage reports
const usageReportRate = new Rate('usage_report_rate'); // Rate of usage reports (successful)
const devicePaused = new Counter('device_paused'); // Times device was paused (STOP/PAUSE command)
const deviceConnected = new Counter('device_connected'); // Successful device connections
const deviceConnectionFailed = new Counter('device_connection_failed'); // Failed device connections

// --- Configuration ---
export const options = {
  setupTimeout: '10m', // Allow up to 10 minutes for setup (device pre-registration)
  scenarios: {
    devices: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '1m', target: 5 },   // warmup
        // { duration: '1m', target: 5000 },
        // { duration: '1m', target: 10000 },
        // { duration: '1m', target: 20000 },
        // { duration: '1m', target: 40000 },
        // { duration: '1m', target: 60000 },
        // { duration: '1m', target: 80000 },
        // { duration: '1m', target: 100000 }, // peak
        // { duration: '5m', target: 100000 },// plateau at max
        { duration: '1m', target: 0 },      // ramp down
      ],
      gracefulRampDown: '2m',
    },
  },
};

const API_BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8080';
const API_DEVICES_BATCH_ENDPOINT = __ENV.API_DEVICES_BATCH_ENDPOINT || '/devices/batch';
const BRIDGE_BASE_URL = __ENV.BRIDGE_BASE_URL || 'http://localhost:3000';
const USAGE_REPORT_INTERVAL = parseInt(__ENV.USAGE_REPORT_INTERVAL || '1'); // seconds between reports
const UNIT_PRICE_MSAT = parseInt(__ENV.UNIT_PRICE_MSAT || '100');
const AUTHORIZE_REQUEST_MSAT = parseInt(__ENV.AUTHORIZE_REQUEST_MSAT || '10000');
const MAX_VUS = parseInt(__ENV.MAX_VUS || '5');

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
  console.log(`Starting load test setup: pre-registering ${MAX_VUS} devices...`);

  // Step 1: Register all devices using batch endpoint
  const batchPayload = JSON.stringify({
    device_id_pattern: 'k6_device_{id}',
    device_secret_pattern: 'k6_device_{id}_password',
    id_start: 1,
    id_end: MAX_VUS,
    id_padding: 6,
    measurement_unit: 'kWh',
    unit_price_msat: UNIT_PRICE_MSAT,
    reporting_strategy: 'interval',
    reporting_interval: USAGE_REPORT_INTERVAL,
    heartbeat_interval: 60,
    authorize_request_msat: AUTHORIZE_REQUEST_MSAT,
    timestamp: getISOTimestamp(),
  });

  const batchRes = http.post(
    `${API_BASE_URL}${API_DEVICES_BATCH_ENDPOINT}`,
    batchPayload,
    { headers: { 'Content-Type': 'application/json' } }
  );

  let registered = 0;
  if (batchRes.status === 204) {
    console.log(`Batch already exists (204 No Content) - all ${MAX_VUS} devices are already registered`);
    registered = MAX_VUS;
  } else if (batchRes.status === 201) {
    const response = JSON.parse(batchRes.body);
    console.log(`Batch creation successful: ${response.devices_created} devices created (range: ${response.id_range})`);
    registered = response.devices_created;
  } else {
    console.error(`Failed to register device batch: ${batchRes.status} - ${batchRes.body}`);
    return {
      registered: 0,
      skipped: 0,
      failed: MAX_VUS,
      total: MAX_VUS,
    };
  }

  console.log(`Setup complete: ${registered} devices registered`);
  return {
    registered,
    skipped: 0,
    failed: 0,
    total: MAX_VUS,
  };
}

// --- Main VU Function ---
export default function () {
  const vuID = __VU;
  const deviceID = generateDeviceID(vuID);
  const deviceSecret = `${deviceID}_password`;

  // Connect device on first iteration only
  if (__ITER === 0) {
    console.log(`[VU ${vuID}] Connecting device ${deviceID}...`);
    const connectPayload = JSON.stringify({
      secret: deviceSecret,
    });

    const connectRes = http.post(
      `${BRIDGE_BASE_URL}/devices/${deviceID}/connect`,
      connectPayload,
      {
        headers: { 'Content-Type': 'application/json' },
        timeout: '120s', // Allow time for invoice + authorization
      }
    );

    if (connectRes.status === 200) {
      deviceConnected.add(1);
      console.log(`[VU ${vuID}] Device ${deviceID} connected and initialized`);
    } else {
      deviceConnectionFailed.add(1);
      console.error(`[VU ${vuID}] Failed to connect ${deviceID}: ${connectRes.status} - ${connectRes.body}`);
      // Continue anyway - might retry on next iteration or fail gracefully
    }
  }

  // k6 calls this function in a loop - each call sends one usage report
  // The bridge handles all the MQTT logic, authorization maintenance, etc.

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

  // Send usage report via bridge
  const usageRes = http.post(
    `${BRIDGE_BASE_URL}/devices/${deviceID}/usage`,
    usagePayload,
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (usageRes.status === 200) {
    usageReported.add(1);
    usageReportRate.add(1);
  } else if (usageRes.status === 423) {
    // 423 = Locked/Reporting disabled (STOP/PAUSE command received)
    // Device is paused, not failed - k6 will continue calling this function
    devicePaused.add(1);
  } else {
    usageReportFailed.add(1);
    console.error(`[VU ${vuID}] Usage report failed: ${usageRes.status} - ${usageRes.body}`);
  }

  // Sleep for a random interval between 0.1 and 1.0 seconds
  // This creates realistic, desynchronized load patterns
  const sleepDuration = 0.1 + Math.random() * 0.9; // Random between 0.1 and 1.0 seconds
  sleep(sleepDuration);
}

// --- Teardown ---
export function teardown(data) {
  // console.log("Disconnecting all devices...");

  // const deviceIDs = data?.deviceIDs || [];
  // let disconnected = 0;
  // let failed = 0;

  // // Disconnect all devices
  // if (deviceIDs.length > 0) {
  //   // Disconnect sequentially
  //   for (const deviceID of deviceIDs) {
  //     const res = http.post(
  //       `${BRIDGE_BASE_URL}/devices/${deviceID}/disconnect`,
  //       '',
  //       { timeout: '10s' }
  //     );
  //     if (res.status === 200) {
  //       disconnected++;
  //     } else if (res.status !== 404) { // 404 is OK, device wasn't connected
  //       failed++;
  //     }
  //   }
  // } else {
  //   // Fallback: try to disconnect devices 1 to MAX_VUS
  //   console.log("No device IDs in data, attempting to disconnect all devices...");
  //   for (let id = 1; id <= MAX_VUS; id++) {
  //     const deviceID = `k6_device_${String(id).padStart(6, '0')}`;
  //     const res = http.post(
  //       `${BRIDGE_BASE_URL}/devices/${deviceID}/disconnect`,
  //       '',
  //       { timeout: '10s' }
  //     );
  //     if (res.status === 200) {
  //       disconnected++;
  //     } else if (res.status !== 404) { // 404 is OK, device wasn't connected
  //       failed++;
  //     }
  //   }
  // }

  // console.log(`Teardown complete: ${disconnected} disconnected, ${failed} failed`);
  console.log("Load test finished.");
}
