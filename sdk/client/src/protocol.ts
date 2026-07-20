export const MELDBASE_PROTOCOL_VERSION = 1 as const;

export interface ProtocolDescriptor {
  readonly versions: readonly number[];
  readonly capabilities: readonly string[];
}

const CAPABILITY_PATTERN = /^[a-z][a-z0-9]*(?:[._][a-z][a-z0-9_]*){0,7}$/;

export function decodeProtocolDescriptor(input: unknown): ProtocolDescriptor {
  if (!record(input) || !exactKeys(input, ["versions", "capabilities"]) ||
      !Array.isArray(input.versions) || input.versions.length === 0 || input.versions.length > 8 ||
      !Array.isArray(input.capabilities) || input.capabilities.length > 64) {
    throw new Error("Malformed Meldbase protocol descriptor");
  }
  const versions: number[] = [];
  for (const version of input.versions) {
    if (!Number.isSafeInteger(version) || version <= 0 || version > 65_535 ||
        (versions.length > 0 && version <= versions[versions.length - 1]!)) {
      throw new Error("Malformed Meldbase protocol versions");
    }
    versions.push(version);
  }
  const capabilities: string[] = [];
  for (const capability of input.capabilities) {
    if (typeof capability !== "string" || capability.length > 64 || !CAPABILITY_PATTERN.test(capability) ||
        (capabilities.length > 0 && capability <= capabilities[capabilities.length - 1]!)) {
      throw new Error("Malformed Meldbase protocol capabilities");
    }
    capabilities.push(capability);
  }
  return Object.freeze({
    versions: Object.freeze(versions),
    capabilities: Object.freeze(capabilities),
  });
}

export function supportsProtocol(descriptor: ProtocolDescriptor, version: number, required: readonly string[] = []): boolean {
  if (!descriptor.versions.includes(version)) return false;
  return required.every((capability) => descriptor.capabilities.includes(capability));
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}
