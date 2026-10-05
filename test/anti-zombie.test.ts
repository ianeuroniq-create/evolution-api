/**
 * Anti-zombie patches: event-queue step timeout + connection heartbeat.
 *
 * Run: npx tsx test/anti-zombie.test.ts
 * (plain node:assert; the repo has no unit-test runner)
 */
import assert from 'node:assert/strict';

import { ConnectionHeartbeat, HeartbeatClient } from '../src/utils/connectionHeartbeat';
import { EventQueueRunner, HISTORY_SYNC_TIMEOUT_MULTIPLIER } from '../src/utils/eventQueueRunner';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const never = () => new Promise<void>(() => {});

let failed = 0;
async function test(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    console.log(`ok   - ${name}`);
  } catch (error) {
    failed++;
    console.error(`FAIL - ${name}\n`, error);
  }
}

function makeRunner(overrides: Partial<{ STEP_TIMEOUT_MS: number; MAX_CONSECUTIVE_TIMEOUTS: number }> = {}) {
  const log = { errors: [] as unknown[], timeouts: [] as number[], late: [] as number[], stalled: 0, recovered: 0 };
  const runner = new EventQueueRunner(
    { STEP_TIMEOUT_MS: 40, MAX_CONSECUTIVE_TIMEOUTS: 3, ...overrides },
    {
      onError: (e) => log.errors.push(e),
      onTimeout: (i) => log.timeouts.push(i.consecutive),
      onLateFinish: (i) => log.late.push(i.elapsedMs),
      onStalled: () => log.stalled++,
      onRecovered: () => log.recovered++,
    },
  );
  return { runner, log };
}

// Chains steps exactly like BaileysStartupService.eventHandler() does.
function makeQueue(runner: EventQueueRunner) {
  let queue: Promise<void> = Promise.resolve();
  return {
    push(step: () => Promise<unknown>, events: string[] = ['messages.upsert']) {
      queue = queue.then(() => runner.runStep(step, events).then(() => undefined)).catch(() => undefined);
      return queue;
    },
    get tail() {
      return queue;
    },
  };
}

async function main() {
  // The runner/heartbeat timers are unref'd (as in production, where the HTTP
  // server keeps the loop alive): keep the test process alive explicitly.
  const keepAlive = setInterval(() => undefined, 1000);

  // ---------------------------------------------------------------- PATCH 1
  await test('a never-resolving step does not block the next batch', async () => {
    const { runner, log } = makeRunner();
    const queue = makeQueue(runner);
    const ran: string[] = [];

    queue.push(never);
    queue.push(async () => {
      ran.push('second');
    });
    await queue.tail;

    assert.deepEqual(ran, ['second']);
    assert.deepEqual(log.timeouts, [1]);
    assert.equal(runner.consecutiveTimeouts, 0, 'a step finishing in time resets the counter');
  });

  await test('counter increments on consecutive timeouts, stalls once, recovers', async () => {
    const { runner, log } = makeRunner();
    const queue = makeQueue(runner);

    for (let i = 0; i < 4; i++) queue.push(never);
    await queue.tail;
    assert.deepEqual(log.timeouts, [1, 2, 3, 4]);
    assert.equal(log.stalled, 1, 'onStalled fires once when the threshold is crossed');
    assert.equal(runner.isStalled, true);

    queue.push(async () => undefined);
    await queue.tail;
    assert.equal(runner.consecutiveTimeouts, 0);
    assert.equal(runner.isStalled, false);
    assert.equal(log.recovered, 1);
  });

  await test('a timed-out step that finishes later is reported (late finish)', async () => {
    const { runner, log } = makeRunner();
    const queue = makeQueue(runner);
    queue.push(() => sleep(80));
    await queue.tail;
    assert.deepEqual(log.timeouts, [1]);
    await sleep(80);
    assert.equal(log.late.length, 1);
    assert.ok(log.late[0] >= 70);
  });

  await test('history sync batches get a 5x timeout', async () => {
    const { runner, log } = makeRunner();
    assert.equal(runner.timeoutFor(['messaging-history.set']), 40 * HISTORY_SYNC_TIMEOUT_MULTIPLIER);
    assert.equal(runner.timeoutFor(['messages.upsert']), 40);
    // 80ms > 40ms but < 200ms: a history sync of that duration is NOT a timeout
    const result = await runner.runStep(() => sleep(80), ['messaging-history.set']);
    assert.equal(result, 'done');
    assert.deepEqual(log.timeouts, []);
  });

  await test('a rejecting step or throwing hook never breaks the chain', async () => {
    const runner = new EventQueueRunner(
      { STEP_TIMEOUT_MS: 30, MAX_CONSECUTIVE_TIMEOUTS: 1 },
      {
        onError: () => {
          throw new Error('hook boom');
        },
        onTimeout: () => {
          throw new Error('hook boom');
        },
        onLateFinish: () => undefined,
        onStalled: () => {
          throw new Error('hook boom');
        },
        onRecovered: () => undefined,
      },
    );
    const queue = makeQueue(runner);
    const ran: string[] = [];
    queue.push(async () => {
      throw new Error('step boom');
    });
    queue.push(never);
    queue.push(async () => {
      ran.push('after');
    });
    await queue.tail;
    assert.deepEqual(ran, ['after']);
  });

  await test('reset() clears the streak (used after an auto-restart)', async () => {
    const { runner } = makeRunner({ MAX_CONSECUTIVE_TIMEOUTS: 2 });
    await runner.runStep(never, ['x']);
    await runner.runStep(never, ['x']);
    assert.equal(runner.isStalled, true);
    runner.reset();
    assert.equal(runner.consecutiveTimeouts, 0);
    assert.equal(runner.isStalled, false);
  });

  // ---------------------------------------------------------------- PATCH 4
  const cfg = { ENABLED: true, INTERVAL_MS: 15, MAX_FAILURES: 3, TIMEOUT_MS: 25 };

  function makeSetup(queryImpl: () => Promise<unknown>) {
    const state = { connection: 'open', inFlight: false, endSession: false, isDeleting: false };
    const calls = { query: [] as Array<{ node: any; timeoutMs?: number }>, reconnect: 0, failures: [] as number[] };
    const client: HeartbeatClient = {
      ws: { isOpen: true },
      generateMessageTag: () => `tag-${calls.query.length}`,
      query: (node, timeoutMs) => {
        calls.query.push({ node, timeoutMs });
        return queryImpl();
      },
    };
    let current: HeartbeatClient | undefined = client;
    const hb = new ConnectionHeartbeat(cfg, {
      getClient: () => current,
      canProbe: () => state.connection === 'open' && !state.endSession && !state.isDeleting && !state.inFlight,
      onProbeFailed: ({ failures }) => calls.failures.push(failures),
      // mirrors BaileysStartupService.forceReconnect(): sets the single-flight flag
      onUnhealthy: async () => {
        if (state.inFlight) return;
        state.inFlight = true;
        state.connection = 'connecting';
        calls.reconnect++;
      },
      onError: (e) => {
        throw e;
      },
    });
    return {
      hb,
      state,
      calls,
      client,
      setClient: (c?: HeartbeatClient) => {
        current = c;
      },
    };
  }

  await test('query rejecting 3x triggers forceReconnect exactly once (single-flight)', async () => {
    const { hb, state, calls } = makeSetup(() => Promise.reject(new Error('Timed Out')));
    assert.equal(await hb.tick(), 'failed');
    assert.equal(await hb.tick(), 'failed');
    assert.equal(await hb.tick(), 'reconnect');
    assert.equal(calls.reconnect, 1);
    assert.deepEqual(calls.failures, [1, 2, 3]);
    assert.equal(state.inFlight, true);

    // further ticks while reconnect is in flight do nothing
    for (let i = 0; i < 6; i++) assert.equal(await hb.tick(), 'skipped');
    assert.equal(calls.reconnect, 1);
    assert.equal(calls.query.length, 3);

    // even if the state stays 'open', the in-flight flag blocks a second reconnect
    state.connection = 'open';
    for (let i = 0; i < 6; i++) await hb.tick();
    assert.equal(calls.reconnect, 1);
  });

  await test('sends the Baileys w:p ping with the configured timeout', async () => {
    const { hb, calls } = makeSetup(() => Promise.resolve({ tag: 'iq', attrs: { type: 'result' } }));
    assert.equal(await hb.tick(), 'ok');
    const { node, timeoutMs } = calls.query[0];
    assert.equal(timeoutMs, cfg.TIMEOUT_MS);
    assert.equal(node.tag, 'iq');
    assert.deepEqual(node.attrs, { id: 'tag-0', to: '@s.whatsapp.net', type: 'get', xmlns: 'w:p' });
    assert.deepEqual(node.content, [{ tag: 'ping', attrs: {} }]);
  });

  await test('a success between failures resets the counter', async () => {
    let fail = true;
    const { hb, calls } = makeSetup(() => (fail ? Promise.reject(new Error('x')) : Promise.resolve({})));
    await hb.tick();
    await hb.tick();
    fail = false;
    assert.equal(await hb.tick(), 'ok');
    assert.equal(hb.consecutiveFailures, 0);
    fail = true;
    await hb.tick();
    await hb.tick();
    assert.equal(calls.reconnect, 0);
  });

  await test('closed websocket counts as a failure without calling query', async () => {
    const { hb, calls, client } = makeSetup(() => Promise.resolve({}));
    client.ws = { isOpen: false };
    await hb.tick();
    await hb.tick();
    assert.equal(await hb.tick(), 'reconnect');
    assert.equal(calls.query.length, 0);
    assert.equal(calls.reconnect, 1);
  });

  await test('never probes instances in QR/pairing or ending', async () => {
    const { hb, state, calls } = makeSetup(() => Promise.reject(new Error('x')));
    for (const connection of ['connecting', 'close']) {
      state.connection = connection;
      for (let i = 0; i < 5; i++) assert.equal(await hb.tick(), 'skipped');
    }
    state.connection = 'open';
    state.endSession = true;
    for (let i = 0; i < 5; i++) assert.equal(await hb.tick(), 'skipped');
    state.endSession = false;
    state.isDeleting = true;
    for (let i = 0; i < 5; i++) assert.equal(await hb.tick(), 'skipped');
    assert.equal(calls.query.length, 0);
    assert.equal(calls.reconnect, 0);
  });

  await test('failure of a socket replaced during the probe is ignored', async () => {
    const setup = makeSetup(() => Promise.reject(new Error('Connection Closed')));
    const replacement: HeartbeatClient = { ...setup.client };
    setup.client.query = () => {
      setup.setClient(replacement);
      return Promise.reject(new Error('Connection Closed'));
    };
    assert.equal(await setup.hb.tick(), 'skipped');
    assert.equal(setup.hb.consecutiveFailures, 0);
  });

  await test('overlapping ticks do not double-probe', async () => {
    const { hb, calls } = makeSetup(() => sleep(20).then(() => ({})));
    const [a, b] = await Promise.all([hb.tick(), hb.tick()]);
    assert.deepEqual([a, b].sort(), ['busy', 'ok']);
    assert.equal(calls.query.length, 1);
  });

  await test('start() is restart-safe and stop() clears the timer', async () => {
    const { hb, calls } = makeSetup(() => Promise.resolve({}));
    hb.start();
    hb.start(); // a reload calls start again: must not leave two intervals
    await sleep(cfg.INTERVAL_MS * 4 + 5);
    hb.stop();
    const probes = calls.query.length;
    assert.ok(probes >= 2 && probes <= 5, `expected ~4 probes from one interval, got ${probes}`);
    assert.equal(hb.isRunning, false);
    await sleep(cfg.INTERVAL_MS * 3);
    assert.equal(calls.query.length, probes, 'no probes after stop()');
  });

  await test('disabled heartbeat never starts', async () => {
    const hb = new ConnectionHeartbeat(
      { ...cfg, ENABLED: false },
      {
        getClient: () => undefined,
        canProbe: () => true,
        onProbeFailed: () => undefined,
        onUnhealthy: async () => undefined,
        onError: () => undefined,
      },
    );
    hb.start();
    assert.equal(hb.isRunning, false);
  });

  clearInterval(keepAlive);

  if (failed) {
    console.error(`\n${failed} test(s) failed`);
    process.exit(1);
  }
  console.log('\nall anti-zombie tests passed');
}

main();
