/**
 * Shared doctor types — split out of doctor.ts (BET-Y1Q3-T6-04) so
 * doctor-checks / doctor-report / doctor-run can reference them without
 * circular imports.
 */
export interface Check {
  name: string;
  status: 'ok' | 'warn' | 'fail';
  message: string;
  issues?: Array<{ type: string; skill: string; action: string; fix?: any }>;
  /**
   * v0.36+ brain-health-100: structured remediation jobs per check.
   * Populated by the recommendation generator; consumed by
   * `gbrain doctor --remediation-plan` / `--remediate`. Optional and
   * additive — schema_version stays at 2 (D4).
   */
  remediation?: Array<{
    id: string;
    job: string;
    params: Record<string, unknown>;
    idempotency_key: string;
    severity: 'critical' | 'high' | 'medium' | 'low';
    est_seconds: number;
    est_usd_cost?: number;
    depends_on?: string[];
    rationale: string;
    protected?: boolean;
  }>;
  /** Top-level triage state per D13. */
  remediation_status?: 'remediable' | 'human_only' | 'blocked';
}

/**
 * Structured doctor report. Stable shape consumed by:
 *   - gbrain doctor --json (CLI)
 *   - run_doctor MCP op (remote callers)
 *   - gbrain remote doctor (renders this from the MCP op response)
 *
 * schema_version=2 was set when --json output stabilized; bump only for
 * breaking field changes.
 */
export interface DoctorReport {
  schema_version: 2;
  status: 'healthy' | 'warnings' | 'unhealthy';
  health_score: number;
  checks: Check[];
}

/**
 * Compute the {status, health_score} headline from a list of checks.
 * Mirrors the calculation in outputResults() so remote callers and the
 * existing CLI front-end agree on what "healthy" means.
 */
export function computeDoctorReport(checks: Check[]): DoctorReport {
  const hasFail = checks.some(c => c.status === 'fail');
  const hasWarn = checks.some(c => c.status === 'warn');
  let score = 100;
  for (const c of checks) {
    if (c.status === 'fail') score -= 20;
    else if (c.status === 'warn') score -= 5;
  }
  score = Math.max(0, score);
  const status: DoctorReport['status'] = hasFail ? 'unhealthy' : hasWarn ? 'warnings' : 'healthy';
  return { schema_version: 2, status, health_score: score, checks };
}


/**
 * Render the check list to stdout (JSON or human) and return whether any check failed.
 * Shared between runDoctor and the DB connection-fail early path.
 */
export function outputResults(checks: Check[], json: boolean): boolean {
  const hasFail = checks.some(c => c.status === 'fail');
  const hasWarn = checks.some(c => c.status === 'warn');

  // Compute composite health score (0-100)
  let score = 100;
  for (const c of checks) {
    if (c.status === 'fail') score -= 20;
    else if (c.status === 'warn') score -= 5;
  }
  score = Math.max(0, score);

  if (json) {
    const status = hasFail ? 'unhealthy' : hasWarn ? 'warnings' : 'healthy';
    console.log(JSON.stringify({ schema_version: 2, status, health_score: score, checks }));
    return hasFail;
  }

  console.log('\nGBrain Health Check');
  console.log('===================');
  for (const c of checks) {
    const icon = c.status === 'ok' ? 'OK' : c.status === 'warn' ? 'WARN' : 'FAIL';
    console.log(`  [${icon}] ${c.name}: ${c.message}`);
    if (c.issues) {
      for (const issue of c.issues) {
        console.log(`    → ${issue.type.toUpperCase()}: ${issue.skill}`);
        console.log(`      ACTION: ${issue.action}`);
      }
    }
  }

  if (hasFail) {
    console.log(`\nHealth score: ${score}/100. Failed checks found.`);
  } else if (hasWarn) {
    console.log(`\nHealth score: ${score}/100. All checks OK (some warnings).`);
  } else {
    console.log(`\nHealth score: ${score}/100. All checks passed.`);
  }
  return hasFail;
}
