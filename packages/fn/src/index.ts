import { Counter, Histogram, Registry } from "prom-client";

/**
 * Sol's run-once function contract, mirroring `framework/ocaml/sol-fn`.
 *
 * The function runs exactly once and exits 0 on success, 1 on a handler error,
 * or 130 when SIGTERM/SIGINT interrupts it. A `-fn` is a Kubernetes CronJob (or
 * a Job created from one); this package does not schedule, does not serve HTTP
 * and is not a Lambda runtime.
 */

export type FnOutcome = "ok" | "error" | "cancelled";

export interface RunFnOptions {
  /**
   * The handler. It receives an AbortSignal that aborts on the first
   * SIGTERM/SIGINT, so a cancellable dependency can be stopped; the process
   * still exits 130 if the handler does not return after the abort.
   */
  run: (signal: AbortSignal) => Promise<void> | void;
  /** Pushgateway base URL; defaults to PUSHGATEWAY_URL. No URL means no push. */
  pushgatewayUrl?: string;
  /** Pushgateway job label; defaults to SOL_PUSHGATEWAY_JOB, then "sol-fn". */
  job?: string;
  /** Defaults to process.exit; injectable for tests. */
  exit?: (code: number) => void;
  /** Clock in milliseconds. Defaults to Date.now; injectable for tests. */
  now?: () => number;
  /** Called when the handler fails; the error is still reported by the exit code. */
  onError?: (error: unknown) => void;
  /** Called when a push fails. Defaults to a stderr warning. */
  onPushError?: (message: string) => void;
}

export interface RunFnResult {
  code: 0 | 1 | 130;
  outcome: FnOutcome;
}

const PUSH_PATH = (job: string): string => `/metrics/job/${encodeURIComponent(job)}`;

export async function runFn(opts: RunFnOptions): Promise<RunFnResult> {
  const exit = opts.exit ?? ((code: number) => process.exit(code));
  const now = opts.now ?? Date.now;
  const job = opts.job ?? process.env.SOL_PUSHGATEWAY_JOB ?? "sol-fn";
  const pushgatewayUrl = opts.pushgatewayUrl ?? process.env.PUSHGATEWAY_URL;
  const registry = new Registry();
  const invocations = new Counter({
    name: "sol_fn_invocations_total",
    help: "Total function invocations by status",
    labelNames: ["status"],
    registers: [registry],
  });
  const duration = new Histogram({
    name: "sol_fn_duration_seconds",
    help: "Function run duration in seconds",
    registers: [registry],
  });

  const controller = new AbortController();
  let signalled = false;
  let resolveSignal: (() => void) | undefined;
  const interrupted = new Promise<void>((resolve) => {
    resolveSignal = resolve;
  });
  const onSignal = (): void => {
    if (signalled) return;
    signalled = true;
    controller.abort();
    resolveSignal?.();
  };
  process.once("SIGTERM", onSignal);
  process.once("SIGINT", onSignal);

  const started = now();
  let outcome: FnOutcome;
  let failure: unknown;
  try {
    const run = Promise.resolve()
      .then(() => opts.run(controller.signal))
      .then(() => "ok" as const)
      .catch((error: unknown) => {
        failure = error;
        return "error" as const;
      });
    outcome = await Promise.race([
      run,
      interrupted.then(() => "cancelled" as const),
    ]);
  } finally {
    process.off("SIGTERM", onSignal);
    process.off("SIGINT", onSignal);
  }

  duration.observe(Math.max(0, now() - started) / 1000);
  invocations.inc({ status: outcome });
  if (outcome === "error") {
    opts.onError?.(failure);
  }
  const code = outcome === "ok" ? 0 : outcome === "cancelled" ? 130 : 1;
  if (pushgatewayUrl) {
    await push(registry, pushgatewayUrl, job, opts.onPushError);
  }
  exit(code);
  return { code, outcome };
}

async function push(
  registry: Registry,
  baseUrl: string,
  job: string,
  onError: ((message: string) => void) | undefined,
): Promise<void> {
  const url = `${baseUrl.replace(/\/+$/, "")}${PUSH_PATH(job)}`;
  try {
    const response = await fetch(url, {
      method: "PUT",
      headers: { "content-type": registry.contentType },
      body: await registry.metrics(),
    });
    if (!response.ok) {
      throw new Error(`Pushgateway answered ${response.status}`);
    }
  } catch (error) {
    const message = `sol-fn: push failed; continuing: ${String(error)}`;
    if (onError) onError(message);
    else console.warn(message);
  }
}
