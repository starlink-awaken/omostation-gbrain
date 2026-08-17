/**
 * BET-Y1Q3-T6-04 split the 4659L doctor.ts god-module into multiple files:
 *   - doctor.ts (runDoctor 骨架 + helpers + re-exports)
 *   - doctor-checks.ts (独立 check 函数)
 *   - doctor-db-conn-checks.ts (DB 连接 + embedding 检查)
 *   - doctor-db-data-checks.ts (DB 数据面检查)
 *   - doctor-remediate.ts (remediation plan/execute)
 *   - doctor-types.ts (共享类型)
 *
 * source-grep tests that previously asserted on doctor.ts now read the
 * concatenated doctor surface so structural regressions still fail loudly
 * without a live DB.
 */
const DOCTOR_FILES = [
  'doctor.ts',
  'doctor-checks.ts',
  'doctor-db-conn-checks.ts',
  'doctor-db-data-checks.ts',
  'doctor-remediate.ts',
  'doctor-types.ts',
];

export async function readDoctorSources(): Promise<string> {
  let out = '';
  for (const f of DOCTOR_FILES) {
    out += await Bun.file(new URL(`../../src/commands/${f}`, import.meta.url)).text();
    out += '\n';
  }
  return out;
}

export function readDoctorSourcesSync(): string {
  const fs = require('fs');
  let out = '';
  for (const f of DOCTOR_FILES) {
    out += fs.readFileSync(`src/commands/${f}`, 'utf8');
    out += '\n';
  }
  return out;
}
