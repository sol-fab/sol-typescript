import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { once } from "node:events";
import { runFn } from "../src/index.js";

async function collector(): Promise<{
  server: Server;
  url: string;
  path: () => string;
  body: () => string;
  close: () => Promise<void>;
}> {
  let receivedPath = "";
  let receivedBody = "";
  const server = createServer((req, res) => {
    receivedPath = req.url ?? "";
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      receivedBody = Buffer.concat(chunks).toString("utf8");
      res.writeHead(200);
      res.end("ok");
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("no test server address");
  }
  return {
    server,
    url: `http://127.0.0.1:${address.port}`,
    path: () => receivedPath,
    body: () => receivedBody,
    close: async () => {
      server.close();
      await once(server, "close");
    },
  };
}

test("a successful invocation exits 0 and pushes ok with duration", async () => {
  const sink = await collector();
  const exits: number[] = [];
  try {
    const result = await runFn({
      run: async () => {},
      pushgatewayUrl: sink.url,
      job: "fn-test",
      exit: (code) => exits.push(code),
    });
    assert.equal(result.code, 0);
    assert.equal(result.outcome, "ok");
    assert.deepEqual(exits, [0]);
    assert.equal(sink.path(), "/metrics/job/fn-test");
    assert.match(sink.body(), /sol_fn_invocations_total\{status="ok"\} 1/);
    assert.match(sink.body(), /sol_fn_duration_seconds_sum/);
  } finally {
    await sink.close();
  }
});

test("a handler error exits 1, reports onError, and pushes error", async () => {
  const sink = await collector();
  const exits: number[] = [];
  let reported: unknown;
  try {
    const result = await runFn({
      run: async () => {
        throw new Error("boom");
      },
      pushgatewayUrl: sink.url,
      job: "fn-error",
      exit: (code) => exits.push(code),
      onError: (error) => {
        reported = error;
      },
    });
    assert.equal(result.code, 1);
    assert.equal(result.outcome, "error");
    assert.deepEqual(exits, [1]);
    assert.equal((reported as Error).message, "boom");
    assert.match(sink.body(), /sol_fn_invocations_total\{status="error"\} 1/);
  } finally {
    await sink.close();
  }
});

test("SIGTERM aborts the handler and exits 130 with cancelled", async () => {
  const sink = await collector();
  const exits: number[] = [];
  const before = process.listenerCount("SIGTERM");
  try {
    const resultPromise = runFn({
      run: (signal) =>
        new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => resolve(), { once: true });
        }),
      pushgatewayUrl: sink.url,
      job: "fn-signal",
      exit: (code) => exits.push(code),
    });
    process.emit("SIGTERM");
    const result = await resultPromise;
    assert.equal(result.code, 130);
    assert.equal(result.outcome, "cancelled");
    assert.deepEqual(exits, [130]);
    assert.match(sink.body(), /sol_fn_invocations_total\{status="cancelled"\} 1/);
    assert.equal(process.listenerCount("SIGTERM"), before);
  } finally {
    await sink.close();
  }
});

test("no push URL means no request and still exits 0", async () => {
  const exits: number[] = [];
  const saved = process.env.PUSHGATEWAY_URL;
  delete process.env.PUSHGATEWAY_URL;
  try {
    const result = await runFn({
      run: async () => {},
      exit: (code) => exits.push(code),
    });
    assert.equal(result.code, 0);
    assert.deepEqual(exits, [0]);
  } finally {
    process.env.PUSHGATEWAY_URL = saved;
    if (saved === undefined) delete process.env.PUSHGATEWAY_URL;
  }
});

test("a failing push is reported and does not change the exit code", async () => {
  const exits: number[] = [];
  const messages: string[] = [];
  const result = await runFn({
    run: async () => {},
    pushgatewayUrl: "http://127.0.0.1:1",
    job: "fn-push-fail",
    exit: (code) => exits.push(code),
    onPushError: (message) => messages.push(message),
  });
  assert.equal(result.code, 0);
  assert.deepEqual(exits, [0]);
  assert.equal(messages.length, 1);
  assert.match(messages[0], /push failed/);
});

test("the job defaults to SOL_PUSHGATEWAY_JOB, then sol-fn", async () => {
  const sink = await collector();
  const saved = process.env.SOL_PUSHGATEWAY_JOB;
  process.env.SOL_PUSHGATEWAY_JOB = "from-env";
  try {
    await runFn({
      run: async () => {},
      pushgatewayUrl: sink.url,
      exit: () => {},
    });
    assert.equal(sink.path(), "/metrics/job/from-env");
  } finally {
    if (saved === undefined) delete process.env.SOL_PUSHGATEWAY_JOB;
    else process.env.SOL_PUSHGATEWAY_JOB = saved;
    await sink.close();
  }
});
