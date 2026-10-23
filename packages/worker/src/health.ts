/**
 * Worker readiness and liveness state.
 *
 * Mirrors `framework/ocaml/sol-worker/lib/worker_health.ml`: readiness means
 * the consumer has joined its group; liveness means a poll/heartbeat was seen
 * within the bound. A rebalance does not make the worker unready, and a quiet
 * topic stays live as long as the consumer heartbeats.
 *
 * The package owns the state and the HTTP responses; the application owns the
 * metrics server and wires the consumer events.
 */

export interface WorkerHealthOptions {
  /** Current time in milliseconds. Defaults to `Date.now`; injectable for tests. */
  now?: () => number;
  /** How long without a poll before the worker is not live. Default 30_000. */
  livenessBoundMs?: number;
}

export interface WorkerHealthResponse {
  status: number;
  body: string;
}

export interface WorkerHealth {
  /** Called on a group join/rebalance; `owned` is the assignment size. */
  onAssignment(owned: number): void;
  /** Called on each consumer heartbeat or successful poll. */
  onPoll(): void;
  isReady(): boolean;
  isLive(): boolean;
  assignedPartitions(): number;
  /** The response for `/readyz` or `/livez`; `undefined` for any other path. */
  response(path: string): WorkerHealthResponse | undefined;
}

const READY = "ready\n";
const NOT_READY = "not ready: the consumer has not joined its consumer group yet\n";
const LIVE = "live\n";
const NOT_LIVE = "not live: no successful poll recently\n";

export function createWorkerHealth(opts: WorkerHealthOptions = {}): WorkerHealth {
  const now = opts.now ?? Date.now;
  const livenessBoundMs = opts.livenessBoundMs ?? 30_000;
  let joined = false;
  let assigned = 0;
  let lastPoll = now();

  return {
    onAssignment(owned: number): void {
      assigned = Number.isFinite(owned) && owned > 0 ? Math.floor(owned) : 0;
      joined = true;
    },
    onPoll(): void {
      lastPoll = now();
    },
    isReady(): boolean {
      return joined;
    },
    isLive(): boolean {
      return now() - lastPoll <= livenessBoundMs;
    },
    assignedPartitions(): number {
      return assigned;
    },
    response(path: string): WorkerHealthResponse | undefined {
      if (path === "/readyz") {
        return joined
          ? { status: 200, body: READY }
          : { status: 503, body: NOT_READY };
      }
      if (path === "/livez") {
        return now() - lastPoll <= livenessBoundMs
          ? { status: 200, body: LIVE }
          : { status: 503, body: NOT_LIVE };
      }
      return undefined;
    },
  };
}
