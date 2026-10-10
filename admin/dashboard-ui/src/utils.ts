import type { RecordValue } from "./types";

/** Formats dashboard numbers without fractional digits. */
const integer = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
/** Formats dashboard numbers with at most one fractional digit. */
const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });

/** Returns a non-array object or an empty fallback for missing dashboard data. */
export function object(value: unknown): RecordValue {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as RecordValue : {};
}

/** Reads a dotted path through dashboard data objects. */
export function valueAt(source: unknown, path: string): unknown {
  let current: unknown = source;
  for (const segment of path.split(".")) current = object(current)[segment];
  return current;
}

/** Converts a value to a finite number or zero. */
export function number(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

/** Formats a dashboard count as an integer. */
export function count(value: unknown): string {
  return integer.format(number(value));
}

/** Formats a valid rate or reports that samples are still warming up. */
export function rate(value: unknown, valid: unknown): string {
  return valid ? decimal.format(number(value)) : "warming up";
}

/** Formats a multiplier when its denominator is positive. */
export function ratio(numerator: unknown, denominator: unknown): string {
  const bottom = number(denominator);
  return bottom > 0 ? `${decimal.format(number(numerator) / bottom)}×` : "no results";
}

/** Formats a byte count using binary size units. */
export function bytes(value: unknown): string {
  const amount = number(value);
  if (amount < 1024) return `${count(amount)} B`;
  if (amount < 1024 ** 2) return `${decimal.format(amount / 1024)} KiB`;
  if (amount < 1024 ** 3) return `${decimal.format(amount / 1024 ** 2)} MiB`;
  return `${decimal.format(amount / 1024 ** 3)} GiB`;
}

/** Formats a nanosecond duration in a readable time unit. */
export function duration(value: unknown): string {
  const nanos = number(value);
  if (nanos < 1_000) return `${count(nanos)} ns`;
  if (nanos < 1_000_000) return `${decimal.format(nanos / 1_000)} µs`;
  if (nanos < 1_000_000_000) return `${decimal.format(nanos / 1_000_000)} ms`;
  return `${decimal.format(nanos / 1_000_000_000)} s`;
}

/** Formats nanosecond uptime as days, hours, or minutes. */
export function uptime(value: unknown): string {
  let seconds = Math.floor(number(value) / 1_000_000_000);
  const days = Math.floor(seconds / 86_400);
  seconds %= 86_400;
  const hours = Math.floor(seconds / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  return days ? `${days}d ${hours}h` : hours ? `${hours}h ${minutes}m` : `${minutes}m`;
}

/** Formats a valid timestamp as local time or returns a missing-value marker. */
export function time(value: unknown): string {
  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? "—" : parsed.toLocaleTimeString();
}

/** Formats an available fraction as a percentage or reports missing samples. */
export function percent(value: unknown, available: unknown): string {
  return available ? `${decimal.format(number(value) * 100)}%` : "no samples";
}
