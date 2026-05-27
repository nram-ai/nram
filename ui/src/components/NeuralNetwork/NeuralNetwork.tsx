import { useEffect, useRef } from "react";
import { useReducedMotion } from "../../hooks/useReducedMotion";
import { subscribe, currentBaselineRate } from "./networkBus";
import styles from "./NeuralNetwork.module.css";

type Node = {
  x: number;
  y: number;
  baseY: number;
  vx: number;
  vy: number;
  band: number;
  size: number;
};

type Pulse = {
  fromIdx: number;
  toIdx: number;
  startedAt: number;
  duration: number;
};

const BAND_OFFSETS = [0.15, 0.38, 0.62, 0.85] as const;
const BAND_WEIGHTS = [0.25, 0.3, 0.3, 0.15] as const;
const EDGE_DISTANCE = 160;
const SAME_BAND_OPACITY = 0.3;
const MAX_PULSES = 8;
const PULSE_DURATION_MS = 600;

function readVar(name: string, fallback: string): string {
  if (typeof window === "undefined") return fallback;
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

export function NeuralNetwork() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const reducedMotion = useReducedMotion();

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let nodes: Node[] = [];
    let pulses: Pulse[] = [];
    let rafId = 0;
    let lastFrame = performance.now();
    let paused = false;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);

    const resize = () => {
      const w = window.innerWidth;
      const h = window.innerHeight;
      canvas.width = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      canvas.style.width = `${w}px`;
      canvas.style.height = `${h}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    const initNodes = () => {
      nodes = [];
      const w = window.innerWidth;
      const h = window.innerHeight;
      const total = Math.min(48, Math.floor(w / 36));
      for (let b = 0; b < BAND_OFFSETS.length; b++) {
        const bandCount = Math.max(2, Math.round(total * BAND_WEIGHTS[b]));
        const baseY = h * BAND_OFFSETS[b];
        const jitter = h * 0.06;
        for (let i = 0; i < bandCount; i++) {
          const y = baseY + (Math.random() - 0.5) * jitter * 2;
          nodes.push({
            x: Math.random() * w,
            y,
            baseY: y,
            vx: (Math.random() - 0.5) * 0.1,
            vy: (Math.random() - 0.5) * 0.03,
            band: b,
            size: Math.random() * 1.3 + 0.7,
          });
        }
      }
    };

    const pickCrossBandEdge = (): [number, number] | null => {
      if (nodes.length < 2) return null;
      // Try up to 12 random pairs to find a cross-band one.
      for (let tries = 0; tries < 12; tries++) {
        const a = Math.floor(Math.random() * nodes.length);
        const b = Math.floor(Math.random() * nodes.length);
        if (a === b) continue;
        if (Math.abs(nodes[a].band - nodes[b].band) !== 1) continue;
        return [a, b];
      }
      // Fallback to any pair so the system never stalls.
      const a = Math.floor(Math.random() * nodes.length);
      let b = Math.floor(Math.random() * nodes.length);
      if (b === a) b = (a + 1) % nodes.length;
      return [a, b];
    };

    const firePulse = (originBand?: number, count = 1) => {
      for (let i = 0; i < count; i++) {
        let edge: [number, number] | null = null;
        if (originBand !== undefined) {
          // Prefer an edge starting in the requested band.
          const candidates = nodes
            .map((n, idx) => ({ idx, band: n.band }))
            .filter((n) => n.band === originBand);
          if (candidates.length > 0) {
            const from = candidates[Math.floor(Math.random() * candidates.length)];
            const targets = nodes
              .map((n, idx) => ({ idx, band: n.band }))
              .filter((n) => Math.abs(n.band - from.band) === 1);
            if (targets.length > 0) {
              const to = targets[Math.floor(Math.random() * targets.length)];
              edge = [from.idx, to.idx];
            }
          }
        }
        if (!edge) edge = pickCrossBandEdge();
        if (!edge) continue;
        if (pulses.length >= MAX_PULSES) pulses.shift();
        pulses.push({
          fromIdx: edge[0],
          toIdx: edge[1],
          startedAt: performance.now(),
          duration: PULSE_DURATION_MS,
        });
      }
    };

    const draw = (now: number) => {
      const dt = now - lastFrame;
      lastFrame = now;

      const w = window.innerWidth;
      const h = window.innerHeight;
      ctx.clearRect(0, 0, w, h);

      // Resolve theme-driven colors once per frame. Cheap; getPropertyValue
      // is essentially a hash lookup.
      const edgeColor = readVar("--network-edge", "201 92% 74%");
      const nodeColor = readVar("--network-node", "201 92% 74%");
      const pulseColor = readVar("--network-pulse", "201 94% 82%");

      // Drift.
      for (const n of nodes) {
        n.x += n.vx;
        n.y += n.vy;
        if (n.x < 0) {
          n.x = 0;
          n.vx *= -1;
        } else if (n.x > w) {
          n.x = w;
          n.vx *= -1;
        }
        // Soft tether to baseY so bands stay legible even after many frames.
        const drift = n.y - n.baseY;
        if (Math.abs(drift) > h * 0.06) {
          n.vy *= -1;
        }
      }

      // Edges.
      ctx.lineWidth = 0.5;
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const a = nodes[i];
          const b = nodes[j];
          const bandDiff = Math.abs(a.band - b.band);
          if (bandDiff > 1) continue;
          const dx = a.x - b.x;
          const dy = a.y - b.y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          if (dist >= EDGE_DISTANCE) continue;
          const proximity = 1 - dist / EDGE_DISTANCE;
          const baseOpacity = proximity * 0.12;
          const opacity = bandDiff === 0 ? baseOpacity * SAME_BAND_OPACITY : baseOpacity;
          ctx.strokeStyle = `hsla(${edgeColor} / ${opacity})`;
          ctx.beginPath();
          ctx.moveTo(a.x, a.y);
          ctx.lineTo(b.x, b.y);
          ctx.stroke();
        }
      }

      // Nodes.
      ctx.fillStyle = `hsla(${nodeColor} / 0.6)`;
      for (const n of nodes) {
        ctx.beginPath();
        ctx.arc(n.x, n.y, n.size, 0, Math.PI * 2);
        ctx.fill();
      }

      // Pulses.
      const survivors: Pulse[] = [];
      for (const p of pulses) {
        const elapsed = now - p.startedAt;
        if (elapsed >= p.duration) continue;
        const tRaw = elapsed / p.duration;
        // Ease-in-out so the dot accelerates and decelerates along the edge.
        const t = tRaw < 0.5 ? 2 * tRaw * tRaw : 1 - Math.pow(-2 * tRaw + 2, 2) / 2;
        const a = nodes[p.fromIdx];
        const b = nodes[p.toIdx];
        if (!a || !b) continue;
        const px = a.x + (b.x - a.x) * t;
        const py = a.y + (b.y - a.y) * t;
        // Trailing comet: a small line behind the head, fading out.
        const trailT = Math.max(0, tRaw - 0.18);
        const tx = a.x + (b.x - a.x) * trailT;
        const ty = a.y + (b.y - a.y) * trailT;
        const grad = ctx.createLinearGradient(tx, ty, px, py);
        grad.addColorStop(0, `hsla(${pulseColor} / 0)`);
        grad.addColorStop(1, `hsla(${pulseColor} / 0.85)`);
        ctx.strokeStyle = grad;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(tx, ty);
        ctx.lineTo(px, py);
        ctx.stroke();
        // Head.
        ctx.fillStyle = `hsla(${pulseColor} / 0.95)`;
        ctx.beginPath();
        ctx.arc(px, py, 2.4, 0, Math.PI * 2);
        ctx.fill();
        survivors.push(p);
      }
      pulses = survivors;

      // Baseline emission. Per-frame probability derives from the current
      // baseline rate so the cadence is correct regardless of frame timing.
      const baselineRate = currentBaselineRate();
      // Jitter ±30% so the cadence does not feel metronomic.
      const jitter = 1 + (Math.random() - 0.5) * 0.6;
      const expected = (baselineRate * dt) / 1000;
      if (Math.random() < expected * jitter) {
        firePulse();
      }

      rafId = requestAnimationFrame(draw);
    };

    const handleResize = () => {
      resize();
      initNodes();
    };

    const handleVisibility = () => {
      if (document.hidden) {
        if (!paused) {
          cancelAnimationFrame(rafId);
          paused = true;
        }
      } else if (paused) {
        paused = false;
        lastFrame = performance.now();
        rafId = requestAnimationFrame(draw);
      }
    };

    resize();
    initNodes();

    if (reducedMotion) {
      // Render one static frame; no rAF loop.
      lastFrame = performance.now();
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      // One pass of edges + nodes, no pulses.
      const edgeColor = readVar("--network-edge", "201 92% 74%");
      const nodeColor = readVar("--network-node", "201 92% 74%");
      ctx.lineWidth = 0.5;
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const a = nodes[i];
          const b = nodes[j];
          const bandDiff = Math.abs(a.band - b.band);
          if (bandDiff > 1) continue;
          const dx = a.x - b.x;
          const dy = a.y - b.y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          if (dist >= EDGE_DISTANCE) continue;
          const proximity = 1 - dist / EDGE_DISTANCE;
          const baseOpacity = proximity * 0.12;
          const opacity = bandDiff === 0 ? baseOpacity * SAME_BAND_OPACITY : baseOpacity;
          ctx.strokeStyle = `hsla(${edgeColor} / ${opacity})`;
          ctx.beginPath();
          ctx.moveTo(a.x, a.y);
          ctx.lineTo(b.x, b.y);
          ctx.stroke();
        }
      }
      ctx.fillStyle = `hsla(${nodeColor} / 0.6)`;
      for (const n of nodes) {
        ctx.beginPath();
        ctx.arc(n.x, n.y, n.size, 0, Math.PI * 2);
        ctx.fill();
      }
      window.addEventListener("resize", handleResize);
      return () => window.removeEventListener("resize", handleResize);
    }

    const unsubscribe = subscribe((e) => firePulse(e.originBand, e.count));

    rafId = requestAnimationFrame(draw);
    window.addEventListener("resize", handleResize);
    document.addEventListener("visibilitychange", handleVisibility);

    return () => {
      cancelAnimationFrame(rafId);
      window.removeEventListener("resize", handleResize);
      document.removeEventListener("visibilitychange", handleVisibility);
      unsubscribe();
    };
  }, [reducedMotion]);

  return (
    <div className={styles.shell} aria-hidden>
      <canvas ref={canvasRef} className={styles.canvas} />
    </div>
  );
}
