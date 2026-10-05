import type { BinaryNode } from 'baileys';
import { S_WHATSAPP_NET } from 'baileys';

/**
 * Anti-zombie heartbeat for a Baileys socket, living OUTSIDE the event queue.
 *
 * Every interval it sends the same `w:p` iq ping Baileys' own keepalive uses and
 * waits for the server reply. After MAX_FAILURES consecutive failures it asks the
 * owner to force a reconnect. The owner decides eligibility (`canProbe`): only a
 * connected ('open') instance that is not ending/being deleted/reconnecting is
 * probed, so instances in QR/pairing are never touched.
 */

export type HeartbeatConfig = {
  ENABLED: boolean;
  INTERVAL_MS: number;
  MAX_FAILURES: number;
  TIMEOUT_MS: number;
};

/** The subset of the Baileys socket the heartbeat needs. */
export type HeartbeatClient = {
  ws?: { isOpen?: boolean };
  generateMessageTag: () => string;
  query: (node: BinaryNode, timeoutMs?: number) => Promise<unknown>;
};

export type HeartbeatHooks = {
  getClient: () => HeartbeatClient | undefined;
  canProbe: () => boolean;
  onProbeFailed: (info: { failures: number; maxFailures: number; error: unknown }) => void;
  onUnhealthy: (reason: string) => Promise<void>;
  onError: (error: unknown) => void;
};

export type HeartbeatTickResult = 'skipped' | 'busy' | 'ok' | 'failed' | 'reconnect';

export function buildPingNode(id: string): BinaryNode {
  return {
    tag: 'iq',
    attrs: { id, to: S_WHATSAPP_NET, type: 'get', xmlns: 'w:p' },
    content: [{ tag: 'ping', attrs: {} }],
  };
}

export class ConnectionHeartbeat {
  private timer: NodeJS.Timeout | null = null;
  private failures = 0;
  private ticking = false;

  constructor(
    private readonly config: HeartbeatConfig,
    private readonly hooks: HeartbeatHooks,
  ) {}

  public get consecutiveFailures() {
    return this.failures;
  }

  public get isRunning() {
    return this.timer !== null;
  }

  /** Restart-safe: always clears a previous timer first. */
  public start() {
    this.stop();
    if (!this.config?.ENABLED) return;

    this.timer = setInterval(() => {
      this.tick().catch((error) => this.hooks.onError(error));
    }, this.config.INTERVAL_MS);
    this.timer.unref?.();
  }

  public stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.failures = 0;
  }

  public async tick(): Promise<HeartbeatTickResult> {
    // A probe can take up to TIMEOUT_MS; never overlap two probes.
    if (this.ticking) return 'busy';

    if (!this.hooks.canProbe()) {
      this.failures = 0;
      return 'skipped';
    }

    this.ticking = true;
    const client = this.hooks.getClient();
    try {
      if (!client?.ws?.isOpen) {
        throw new Error('websocket not open');
      }

      await client.query(buildPingNode(client.generateMessageTag()), this.config.TIMEOUT_MS);
      this.failures = 0;
      return 'ok';
    } catch (error) {
      // The world may have changed while we waited (logout, reconnect, new socket):
      // a failure of a socket that is no longer the current one is not a signal.
      if (!this.hooks.canProbe() || this.hooks.getClient() !== client) {
        this.failures = 0;
        return 'skipped';
      }

      this.failures++;
      this.hooks.onProbeFailed({ failures: this.failures, maxFailures: this.config.MAX_FAILURES, error });

      if (this.failures >= this.config.MAX_FAILURES) {
        this.failures = 0;
        await this.hooks.onUnhealthy('heartbeat_failed');
        return 'reconnect';
      }

      return 'failed';
    } finally {
      this.ticking = false;
    }
  }
}
