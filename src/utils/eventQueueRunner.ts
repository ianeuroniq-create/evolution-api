/**
 * Runs one step of the serialized Baileys event queue with a timeout.
 *
 * The queue is a promise chain: if one step never settles, every later batch
 * (including `connection.update` and `messages.upsert`) waits forever and the
 * instance becomes a "zombie" (state 'open', no messages flowing). This runner
 * races each step against a timer: on timeout the chain moves on while the
 * hung step keeps running in the background (its late completion is reported).
 * After N consecutive timeouts the queue is considered stalled.
 */

export type EventQueueRunnerConfig = {
  STEP_TIMEOUT_MS: number;
  MAX_CONSECUTIVE_TIMEOUTS: number;
  /**
   * true  = on timeout the chain moves on while the hung step keeps running.
   * false = observe only: the timeout is reported but the chain still waits for
   *         the step (exactly the behaviour without this runner).
   */
  RELEASE_ON_TIMEOUT?: boolean;
};

export type EventQueueTimeoutInfo = {
  events: string[];
  timeoutMs: number;
  consecutive: number;
};

export type EventQueueRunnerHooks = {
  onError: (error: unknown) => void;
  onTimeout: (info: EventQueueTimeoutInfo) => void;
  onLateFinish: (info: { events: string[]; elapsedMs: number }) => void;
  /** Called once when the consecutive-timeout threshold is reached. */
  onStalled: (info: EventQueueTimeoutInfo) => void;
  /** Called when a step completes in time after the queue was stalled. */
  onRecovered: (info: { events: string[] }) => void;
};

// History sync batches are legitimately slow (thousands of messages written to the DB).
export const HISTORY_SYNC_EVENT = 'messaging-history.set';
export const HISTORY_SYNC_TIMEOUT_MULTIPLIER = 5;

export type EventQueueStepResult = 'done' | 'timeout';

export class EventQueueRunner {
  private consecutive = 0;
  private stalled = false;

  constructor(
    private readonly config: EventQueueRunnerConfig,
    private readonly hooks: EventQueueRunnerHooks,
  ) {}

  public get consecutiveTimeouts() {
    return this.consecutive;
  }

  public get isStalled() {
    return this.stalled;
  }

  /** Forget the timeout streak (e.g. after the client was restarted). */
  public reset() {
    this.consecutive = 0;
    this.stalled = false;
  }

  public timeoutFor(events: string[]): number {
    const base = this.config.STEP_TIMEOUT_MS;
    return events.includes(HISTORY_SYNC_EVENT) ? base * HISTORY_SYNC_TIMEOUT_MULTIPLIER : base;
  }

  public async runStep(step: () => Promise<unknown>, events: string[]): Promise<EventQueueStepResult> {
    const timeoutMs = this.timeoutFor(events);
    const startedAt = Date.now();

    let finished = false;
    let timedOut = false;

    // The step has its own try/catch; this catch is a safety net so a rejection
    // can never break the chain nor become an unhandled rejection.
    const stepPromise = Promise.resolve()
      .then(step)
      .catch((error) => this.safeHook(() => this.hooks.onError(error)))
      .finally(() => {
        finished = true;
        if (timedOut) {
          this.safeHook(() => this.hooks.onLateFinish({ events, elapsedMs: Date.now() - startedAt }));
        }
      });

    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), timeoutMs);
      timer.unref?.();
    });

    const result = await Promise.race([stepPromise.then(() => 'done' as const), timeout]);
    clearTimeout(timer);

    if (result === 'done' || finished) {
      this.consecutive = 0;
      if (this.stalled) {
        this.stalled = false;
        this.safeHook(() => this.hooks.onRecovered({ events }));
      }
      return 'done';
    }

    timedOut = true;
    this.consecutive++;
    const info = { events, timeoutMs, consecutive: this.consecutive };
    this.safeHook(() => this.hooks.onTimeout(info));

    if (!this.stalled && this.consecutive >= this.config.MAX_CONSECUTIVE_TIMEOUTS) {
      this.stalled = true;
      this.safeHook(() => this.hooks.onStalled(info));
    }

    if (!this.config.RELEASE_ON_TIMEOUT) {
      // Observe mode: keep the original ordering guarantee and wait for the step.
      await stepPromise;
    }

    return 'timeout';
  }

  // A throwing hook must never reject: a rejected step would break the promise
  // chain (later `.then` links are skipped) or surface as an unhandled rejection.
  private safeHook(fn: () => void) {
    try {
      fn();
    } catch {
      // ignore — hooks are logging only
    }
  }
}
