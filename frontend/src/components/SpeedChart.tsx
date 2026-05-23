import { useMemo } from "react";
import type { SpeedHistorySample } from "../api/types";

type Props = {
  samples: SpeedHistorySample[];
  /** Configured global throttle (bytes/sec, 0 = unlimited). Drawn as a dashed reference line. */
  capBytesPerSec: number;
  /** All-time observed peak (bytes/sec). Shown as a label and a horizontal hint line. */
  peakAllTimeBytesPerSec: number;
  /** Window peak (bytes/sec). Highlighted as a marker on the chart. */
  peakWindowBytesPerSec: number;
  /** Resolution of each sample in seconds; only used for the X-axis label. */
  resolutionSeconds: number;
  height?: number;
};

const MB = 1024 * 1024;

// SpeedChart renders the throughput history as a filled line chart
// with a Y axis in MB/s, dashed horizontal lines for the configured
// cap and the all-time peak, and a marker on the window peak.
//
// Pure SVG — no chart-library dependency. Layout is responsive: the
// viewBox is 1000 wide and the parent stretches it.
export default function SpeedChart({
  samples,
  capBytesPerSec,
  peakAllTimeBytesPerSec,
  peakWindowBytesPerSec,
  resolutionSeconds,
  height = 220,
}: Props) {
  const W = 1000;
  const H = height;
  const padL = 56;
  const padR = 16;
  const padT = 12;
  const padB = 28;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const layout = useMemo(() => {
    if (samples.length === 0) {
      return { points: "", area: "", yMax: 0, peakX: null as number | null, peakY: null as number | null };
    }
    const maxSample = Math.max(...samples.map((s) => s.bytes_per_sec));
    // The Y axis must accommodate both the data and the cap so the
    // cap line is always visible.
    const yMaxBytes = Math.max(maxSample, capBytesPerSec, peakAllTimeBytesPerSec, 1);
    const niceMax = niceYTop(yMaxBytes / MB) * MB;
    const xStep = innerW / Math.max(samples.length - 1, 1);

    const coords = samples.map((s, i) => {
      const x = padL + i * xStep;
      const y = padT + innerH - (s.bytes_per_sec / niceMax) * innerH;
      return { x, y, v: s.bytes_per_sec };
    });
    const points = coords.map((c) => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");
    const area =
      `M ${coords[0].x.toFixed(1)},${(padT + innerH).toFixed(1)} ` +
      coords.map((c) => `L ${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ") +
      ` L ${coords[coords.length - 1].x.toFixed(1)},${(padT + innerH).toFixed(1)} Z`;

    // Window-peak marker: pick the first sample that hits the max.
    let peakX: number | null = null;
    let peakY: number | null = null;
    if (peakWindowBytesPerSec > 0) {
      const idx = samples.findIndex((s) => s.bytes_per_sec === peakWindowBytesPerSec);
      if (idx >= 0) {
        peakX = coords[idx].x;
        peakY = coords[idx].y;
      }
    }

    return { points, area, yMax: niceMax, peakX, peakY };
  }, [samples, capBytesPerSec, peakAllTimeBytesPerSec, peakWindowBytesPerSec, innerW, innerH, padL, padT]);

  const yTicks = useMemo(() => {
    if (layout.yMax === 0) return [] as { y: number; label: string }[];
    const steps = 4;
    const out: { y: number; label: string }[] = [];
    for (let i = 0; i <= steps; i++) {
      const value = (layout.yMax / steps) * i;
      const y = padT + innerH - (value / layout.yMax) * innerH;
      out.push({ y, label: formatRate(value) });
    }
    return out;
  }, [layout.yMax, padT, innerH]);

  const capY =
    layout.yMax > 0 && capBytesPerSec > 0
      ? padT + innerH - (capBytesPerSec / layout.yMax) * innerH
      : null;
  const peakAllTimeY =
    layout.yMax > 0 && peakAllTimeBytesPerSec > 0
      ? padT + innerH - (peakAllTimeBytesPerSec / layout.yMax) * innerH
      : null;

  const totalSpanSec = samples.length * resolutionSeconds;
  const xAxisLabel = humanizeDuration(totalSpanSec);

  return (
    <svg
      className="speed-chart"
      role="img"
      aria-label={`Download speed over the last ${xAxisLabel}`}
      viewBox={`0 0 ${W} ${H}`}
      preserveAspectRatio="none"
      style={{ width: "100%", height }}
    >
      {/* Grid + Y ticks */}
      {yTicks.map((t, i) => (
        <g key={i}>
          <line
            x1={padL}
            x2={W - padR}
            y1={t.y}
            y2={t.y}
            stroke="var(--border, #2a2a32)"
            strokeWidth={1}
            strokeDasharray={i === 0 ? "0" : "2 4"}
          />
          <text
            x={padL - 6}
            y={t.y + 4}
            textAnchor="end"
            fontSize={11}
            fill="var(--muted, #888)"
          >
            {t.label}
          </text>
        </g>
      ))}

      {/* Data: filled area + line */}
      {samples.length > 0 && (
        <>
          <path d={layout.area} fill="var(--accent, #4f8cff)" fillOpacity={0.18} />
          <polyline
            points={layout.points}
            fill="none"
            stroke="var(--accent, #4f8cff)"
            strokeWidth={1.5}
          />
        </>
      )}

      {/* Cap reference line */}
      {capY !== null && (
        <g>
          <line
            x1={padL}
            x2={W - padR}
            y1={capY}
            y2={capY}
            stroke="var(--warn, #e08e3c)"
            strokeWidth={1.5}
            strokeDasharray="6 4"
          />
          <text
            x={W - padR - 4}
            y={capY - 4}
            textAnchor="end"
            fontSize={11}
            fill="var(--warn, #e08e3c)"
          >
            cap {formatRate(capBytesPerSec)}
          </text>
        </g>
      )}

      {/* All-time peak hint */}
      {peakAllTimeY !== null && (
        <g>
          <line
            x1={padL}
            x2={W - padR}
            y1={peakAllTimeY}
            y2={peakAllTimeY}
            stroke="var(--accent, #4f8cff)"
            strokeWidth={1}
            strokeDasharray="2 6"
            opacity={0.6}
          />
          <text
            x={padL + 4}
            y={peakAllTimeY - 4}
            fontSize={11}
            fill="var(--muted, #888)"
          >
            all-time peak {formatRate(peakAllTimeBytesPerSec)}
          </text>
        </g>
      )}

      {/* Window peak marker */}
      {layout.peakX !== null && layout.peakY !== null && (
        <g>
          <circle
            cx={layout.peakX}
            cy={layout.peakY}
            r={4}
            fill="var(--accent, #4f8cff)"
            stroke="var(--bg, #15151a)"
            strokeWidth={1.5}
          />
        </g>
      )}

      {/* X axis baseline + duration label */}
      <line
        x1={padL}
        x2={W - padR}
        y1={padT + innerH}
        y2={padT + innerH}
        stroke="var(--border, #2a2a32)"
        strokeWidth={1}
      />
      <text
        x={W - padR}
        y={H - 8}
        textAnchor="end"
        fontSize={11}
        fill="var(--muted, #888)"
      >
        last {xAxisLabel}
      </text>
      <text
        x={padL}
        y={H - 8}
        fontSize={11}
        fill="var(--muted, #888)"
      >
        now
      </text>
    </svg>
  );
}

// niceYTop rounds the Y-axis ceiling (in MB/s) up to a friendly value
// so labels read 5, 10, 20, 50, 100 instead of 4.7, 9.3, etc.
function niceYTop(mbps: number): number {
  if (mbps <= 0) return 1;
  const steps = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000];
  for (const s of steps) {
    if (mbps <= s) return s;
  }
  // Beyond 1000 MB/s — round up to the next 1000.
  return Math.ceil(mbps / 1000) * 1000;
}

function formatRate(bytesPerSec: number): string {
  if (bytesPerSec <= 0) return "0 B/s";
  if (bytesPerSec >= MB) return `${(bytesPerSec / MB).toFixed(bytesPerSec >= 10 * MB ? 0 : 1)} MB/s`;
  if (bytesPerSec >= 1024) return `${(bytesPerSec / 1024).toFixed(0)} KB/s`;
  return `${bytesPerSec} B/s`;
}

function humanizeDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)} h`;
  return `${Math.round(seconds / 86400)} d`;
}
