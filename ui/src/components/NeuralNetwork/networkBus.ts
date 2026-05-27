// Pub-sub for the neural-network backdrop. Components anywhere in the tree
// can call firePulse() to push a signal through the network without prop-
// drilling. The NeuralNetwork component is the sole subscriber.
//
// Activity hysteresis: a rolling 10s window tracks the burst rate. When the
// system is busy (>4 bursts/sec), baselineRate drops so real activity is
// what reads. When the system goes quiet (<1 burst/sec), baselineRate
// returns to the floor so the network never goes visually dead.

export type PulseEvent = {
  count: number;
  originBand?: number;
};

type Listener = (e: PulseEvent) => void;

const WINDOW_MS = 10_000;
const FLOOR_RATE = 1.2; // pulses/sec baseline when system is quiet
const SUPPRESSED_RATE = 0.5; // baseline when system is busy
const BUSY_THRESHOLD = 4; // bursts/sec averaged over WINDOW_MS
const QUIET_THRESHOLD = 1;

const listeners = new Set<Listener>();
const burstTimestamps: number[] = [];

function pruneWindow(now: number) {
  const cutoff = now - WINDOW_MS;
  while (burstTimestamps.length > 0 && burstTimestamps[0] < cutoff) {
    burstTimestamps.shift();
  }
}

export function firePulse(originBand?: number, count = 1 + Math.floor(Math.random() * 3)): void {
  const now = Date.now();
  burstTimestamps.push(now);
  pruneWindow(now);
  const event: PulseEvent = { count, originBand };
  for (const fn of listeners) fn(event);
}

export function subscribe(fn: Listener): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function currentBaselineRate(): number {
  const now = Date.now();
  pruneWindow(now);
  const rate = burstTimestamps.length / (WINDOW_MS / 1000);
  if (rate > BUSY_THRESHOLD) return SUPPRESSED_RATE;
  if (rate < QUIET_THRESHOLD) return FLOOR_RATE;
  // Linear ramp between thresholds so the transition does not pop.
  const t = (rate - QUIET_THRESHOLD) / (BUSY_THRESHOLD - QUIET_THRESHOLD);
  return FLOOR_RATE + (SUPPRESSED_RATE - FLOOR_RATE) * t;
}
