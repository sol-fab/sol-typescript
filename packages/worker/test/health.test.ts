import { test } from "node:test";
import assert from "node:assert/strict";
import { createWorkerHealth } from "../src/index.js";

const t0 = 1_000_000;

test("a new worker is live but not ready", () => {
  const health = createWorkerHealth({ now: () => t0 });
  assert.equal(health.isReady(), false);
  assert.equal(health.isLive(), true);
  assert.deepEqual(health.response("/readyz"), {
    status: 503,
    body: "not ready: the consumer has not joined its consumer group yet\n",
  });
  assert.deepEqual(health.response("/livez"), {
    status: 200,
    body: "live\n",
  });
});

test("joining the group makes it ready and records the assignment", () => {
  const health = createWorkerHealth({ now: () => t0 });
  health.onAssignment(3);
  assert.equal(health.isReady(), true);
  assert.equal(health.assignedPartitions(), 3);
  assert.deepEqual(health.response("/readyz"), { status: 200, body: "ready\n" });
});

test("a rebalance keeps readiness and replaces the assignment", () => {
  const health = createWorkerHealth({ now: () => t0 });
  health.onAssignment(3);
  health.onAssignment(1);
  assert.equal(health.isReady(), true);
  assert.equal(health.assignedPartitions(), 1);
});

test("liveness needs a poll within the bound, and a quiet topic can stay live", () => {
  let now = t0;
  const health = createWorkerHealth({ now: () => now, livenessBoundMs: 30_000 });
  now = t0 + 29_999;
  assert.equal(health.isLive(), true);
  health.onPoll();
  now = t0 + 59_999;
  assert.equal(health.isLive(), true);
  now = t0 + 60_000;
  assert.equal(health.isLive(), false);
  assert.deepEqual(health.response("/livez"), {
    status: 503,
    body: "not live: no successful poll recently\n",
  });
});

test("readiness is independent of liveness", () => {
  let now = t0;
  const health = createWorkerHealth({ now: () => now, livenessBoundMs: 1_000 });
  health.onAssignment(1);
  now = t0 + 2_000;
  assert.equal(health.isReady(), true);
  assert.equal(health.isLive(), false);
});

test("other paths are not health responses", () => {
  const health = createWorkerHealth({ now: () => t0 });
  assert.equal(health.response("/metrics"), undefined);
  assert.equal(health.response("/"), undefined);
});
