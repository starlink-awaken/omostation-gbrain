import type { BrainEngine } from '../core/engine.ts';
import type { Check } from './doctor.ts';

export async function runRemediationPlan(
  engine: BrainEngine,
  args: string[],
): Promise<void> {
  const { computeRecommendations, classifyChecks, maxReachableScore } =
    await import('../core/brain-score-recommendations.ts');

  const targetScore = parseIntFlag(args, '--target-score') ?? 90;
  const jsonOutput = args.includes('--json');

  // Cheap path (D7) — don't run slow doctor checks for the plan surface.
  // The recommendation generator works from BrainHealth + context alone.
  const health = await engine.getHealth();
  const ctx = await loadRecommendationContext(engine);
  const recs = computeRecommendations(health, ctx);
  // Synthetic check list for classification — we don't need full doctor
  // output, just the check names the recommendations care about.
  const syntheticChecks = [
    { name: 'brain_score', status: 'ok' as const },
    { name: 'sync_freshness', status: 'ok' as const },
    { name: 'missing_embeddings', status: 'ok' as const },
    { name: 'dead_links', status: 'ok' as const },
    { name: 'orphan_pages', status: 'ok' as const },
  ];
  const classifications = classifyChecks(syntheticChecks, ctx);
  const ceiling = maxReachableScore(health, classifications);

  const filteredRecs = recs.filter((r) => r.status === 'remediable');
  const estTotalSeconds = filteredRecs.reduce((sum, r) => sum + r.est_seconds, 0);
  const estTotalUsd = filteredRecs.reduce((sum, r) => sum + (r.est_usd_cost ?? 0), 0);

  const blocked = classifications
    .filter((c) => c.status === 'blocked')
    .map((c) => ({ check: c.check, reason: c.reason ?? 'prerequisite missing' }));

  const plan = {
    schema_version: 2,
    brain_score_current: health.brain_score,
    brain_score_target: targetScore,
    max_reachable_score: ceiling,
    target_unreachable: targetScore > ceiling,
    plan: filteredRecs.map((r, i) => ({ step: i + 1, ...r })),
    est_total_seconds: estTotalSeconds,
    est_total_usd_cost: Number(estTotalUsd.toFixed(2)),
    blocked,
  };

  if (jsonOutput) {
    console.log(JSON.stringify(plan, null, 2));
    return;
  }

  // Human output
  console.log(`Brain score: ${health.brain_score}/100 → target ${targetScore}`);
  if (plan.target_unreachable) {
    console.log(`Target unreachable: max with autonomous remediation is ${ceiling}/100.`);
  }
  if (plan.plan.length === 0) {
    console.log('No remediations needed. Brain is at target.');
  } else {
    console.log(`Plan: ${plan.plan.length} step(s), est ${plan.est_total_seconds}s, est $${plan.est_total_usd_cost.toFixed(2)}`);
    for (const step of plan.plan) {
      const protectedMark = step.protected ? ' [PROTECTED]' : '';
      const costMark = step.est_usd_cost ? ` ($${step.est_usd_cost.toFixed(2)})` : '';
      console.log(`  ${step.step}. [${step.severity}] ${step.job}${protectedMark} — ${step.rationale}${costMark}`);
    }
  }
  if (blocked.length > 0) {
    console.log(`\nBlocked checks (prereq missing):`);
    for (const b of blocked) {
      console.log(`  - ${b.check}: ${b.reason}`);
    }
  }
}

/**
 * Submit ordered Remediation jobs sequentially per D3, with D5 cascade
 * on failure and D7 scoped recheck between steps.
 *
 * Default behavior: submit-and-wait per step. --dry-run skips submission.
 * --max-usd N refuses if est_total_usd_cost > N. --max-jobs N caps the
 * inner loop.
 *
 * PGLite path: synchronous in-process execution (no durable queue).
 */
export async function runRemediate(
  engine: BrainEngine,
  args: string[],
): Promise<void> {
  const targetScore = parseIntFlag(args, '--target-score') ?? 90;
  const maxJobs = parseIntFlag(args, '--max-jobs') ?? Infinity;
  // A4 amended: --max-cost is an alias for --max-usd. Both spellings are
  // documented as the cron-safety guard. Either threads through to the
  // pre-flight estimate refusal AND, via withBudgetTracker, the mid-run
  // BudgetExhausted hard-throw.
  const maxUsd = parseFloatFlag(args, '--max-usd') ?? parseFloatFlag(args, '--max-cost');
  const dryRun = args.includes('--dry-run');
  const skipConfirm = args.includes('--yes');
  const jsonOutput = args.includes('--json');
  // A4 amended: --resume <plan_hash?> loads the checkpoint for the active
  // (engine,target) and continues from the next step. With no value, the
  // most recent checkpoint for the active engine is loaded.
  const resumeFlagIdx = args.indexOf('--resume');
  const resumeMode = resumeFlagIdx !== -1;
  const resumeArg = resumeMode ? args[resumeFlagIdx + 1] : undefined;
  const resumePlanHash = resumeArg && !resumeArg.startsWith('--') ? resumeArg : undefined;

  const { computeRecommendations, classifyChecks, maxReachableScore } =
    await import('../core/brain-score-recommendations.ts');
  const {
    BudgetTracker,
    BudgetExhausted,
  } = await import('../core/budget/budget-tracker.ts');
  const { withBudgetTracker } = await import('../core/ai/gateway.ts');
  const {
    computePlanHash,
    saveRemediationCheckpoint,
    loadRemediationCheckpoint,
    listRemediationCheckpoints,
    clearRemediationCheckpoint,
  } = await import('../core/remediation-checkpoint.ts');

  const ctx = await loadRecommendationContext(engine);

  // Pre-flight ceiling check (D13)
  const initialHealth = await engine.getHealth();
  const syntheticChecks = [
    { name: 'brain_score', status: 'ok' as const },
    { name: 'sync_freshness', status: 'ok' as const },
    { name: 'missing_embeddings', status: 'ok' as const },
    { name: 'dead_links', status: 'ok' as const },
    { name: 'orphan_pages', status: 'ok' as const },
  ];
  const classifications = classifyChecks(syntheticChecks, ctx);
  const ceiling = maxReachableScore(initialHealth, classifications);
  if (targetScore > ceiling) {
    console.error(
      `[remediate] target ${targetScore} unreachable; max autonomous = ${ceiling}/100. ` +
      `Configure missing prereqs (see --remediation-plan blocked output) or lower --target-score.`,
    );
    process.exit(2);
  }

  // Initial plan
  let recs = computeRecommendations(initialHealth, ctx).filter((r) => r.status === 'remediable');
  if (recs.length === 0) {
    console.log(`Brain at score ${initialHealth.brain_score}/100, target ${targetScore}. Nothing to do.`);
    return;
  }

  // A4 amended: compute plan_hash off the active recommendation ids so the
  // checkpoint binds to THIS plan. Resume only fires for matching plans.
  const planHash = computePlanHash(recs.map((r) => r.id));
  let completedFromCheckpoint = new Set<string>();
  if (resumeMode) {
    const requested = resumePlanHash;
    let cp = requested ? loadRemediationCheckpoint(requested) : null;
    if (!cp && !requested) {
      // No explicit hash: try newest checkpoint that matches the active plan.
      const recent = listRemediationCheckpoints();
      for (const e of recent) {
        const candidate = loadRemediationCheckpoint(e.plan_hash);
        if (candidate && candidate.plan_hash === planHash) {
          cp = candidate;
          break;
        }
      }
    }
    if (!cp) {
      console.error(
        `[remediate --resume] no matching checkpoint found ` +
          `(plan_hash=${planHash}${requested ? `; requested=${requested}` : ''}). ` +
          `Run without --resume to start fresh.`,
      );
      process.exit(2);
    }
    if (cp.plan_hash !== planHash) {
      console.error(
        `[remediate --resume] checkpoint plan_hash=${cp.plan_hash} does not match active plan_hash=${planHash}. ` +
          `The plan has changed (brain state moved). Run without --resume to start fresh.`,
      );
      process.exit(2);
    }
    completedFromCheckpoint = new Set(cp.completed.map((c) => c.id));
    console.error(
      `[remediate --resume] resuming plan_hash=${planHash}: ${completedFromCheckpoint.size} step(s) completed, ` +
        `${recs.length - completedFromCheckpoint.size} remaining.`,
    );
  }

  const estTotalUsd = recs.reduce((sum, r) => sum + (r.est_usd_cost ?? 0), 0);
  if (maxUsd !== null && estTotalUsd > maxUsd) {
    console.error(
      `[remediate] est cost $${estTotalUsd.toFixed(2)} exceeds --max-usd $${maxUsd.toFixed(2)}. Aborting.`,
    );
    process.exit(2);
  }

  if (!skipConfirm && process.stdout.isTTY) {
    console.log(`About to submit ${recs.length} job(s), est ${Math.round(recs.reduce((s, r) => s + r.est_seconds, 0))}s, est $${estTotalUsd.toFixed(2)}`);
    console.log('Pass --yes to proceed (cron-friendly).');
    process.exit(1);
  }

  if (dryRun) {
    console.log(`[remediate --dry-run] Would submit ${recs.length} jobs:`);
    for (const r of recs) console.log(`  - ${r.id} (${r.job})`);
    return;
  }

  // Sequential submit per D3, with D5 cascade on failure and D7
  // scoped recheck between steps.
  const submitted: Array<{ step: number; id: string; job_id: number | null; status: string }> = [];
  const abortedIds = new Set<string>();
  const doctorRunId = crypto.randomUUID();

  const isPGLite = engine.kind === 'pglite';
  if (isPGLite) {
    console.error('[remediate] PGLite engine: running inline (no durable queue).');
  }

  const { MinionQueue } = await import('../core/minions/queue.ts');
  const { waitForCompletion } = await import('../core/minions/wait-for-completion.ts');
  const queue = new MinionQueue(engine);

  // A4 amended: install a BudgetTracker scope around the plan-step loop so
  // any gateway.chat / embed / rerank inside a Minion handler (synthesize,
  // patterns, consolidate) auto-enforces the cap. On BudgetExhausted, the
  // onExhausted callback persists the checkpoint BEFORE the throw propagates;
  // the catch surfaces the actionable --resume hint.
  const remediateTracker = new BudgetTracker({
    label: 'doctor.remediate',
    maxCostUsd: maxUsd ?? undefined,
  });

  let exhaustionSnapshot: { spent: number; cap: number; reason: string; model_id?: string } | undefined;
  remediateTracker.onExhausted(() => {
    // BudgetTracker fires this synchronously from inside reserve()/record()
    // before the throw bubbles. Persist whatever has been done so far.
    const cp = {
      schema_version: 1 as const,
      plan_hash: planHash,
      doctor_run_id: doctorRunId,
      target_score: targetScore,
      started_at: new Date().toISOString(),
      completed: submitted
        .filter((s) => s.status === 'completed')
        .map((s) => ({ id: s.id, job: '', status: s.status, job_id: s.job_id ?? null })),
      aborted_at: new Date().toISOString(),
      abort_reason: 'budget_exhausted' as const,
      budget_snapshot: exhaustionSnapshot,
    };
    saveRemediationCheckpoint(cp);
  });

  const runLoop = async (): Promise<void> => {
    let stepCount = 0;
    while (recs.length > 0 && stepCount < maxJobs) {
      const step = recs[0];
      if (!step) break;
      stepCount++;

      // Resume: skip steps that the checkpoint already marked completed.
      if (completedFromCheckpoint.has(step.id)) {
        submitted.push({ step: stepCount, id: step.id, job_id: null, status: 'completed' });
        recs.shift();
        continue;
      }

      // D5: if depends_on intersects aborted, skip + cascade
      if (step.depends_on && step.depends_on.some((d) => abortedIds.has(d))) {
        submitted.push({ step: stepCount, id: step.id, job_id: null, status: 'skipped_dep_aborted' });
        abortedIds.add(step.id);
        recs.shift();
        continue;
      }

      try {
        const isProtected = !!step.protected;
        const job = await queue.add(
          step.job,
          { ...step.params, doctor_run_id: doctorRunId },
          {
            queue: 'default',
            idempotency_key: step.idempotency_key,
            max_attempts: 2,
            maxWaiting: 1,
          },
          isProtected ? { allowProtectedSubmit: true } : undefined,
        );
        submitted.push({ step: stepCount, id: step.id, job_id: job.id, status: 'submitted' });

        // Wait for terminal state. PGLite is in-process — short poll.
        const terminal = await waitForCompletion(queue, job.id, {
          pollMs: isPGLite ? 250 : 1000,
          timeoutMs: (step.est_seconds + 60) * 1000,
        });
        const lastSub = submitted[submitted.length - 1];
        if (lastSub) lastSub.status = terminal.status;

        if (terminal.status !== 'completed') {
          abortedIds.add(step.id);
        }
      } catch (e) {
        if (e instanceof BudgetExhausted) {
          exhaustionSnapshot = {
            spent: e.spent,
            cap: e.cap,
            reason: e.reason,
            model_id: e.modelId,
          };
          throw e;
        }
        submitted.push({
          step: stepCount, id: step.id, job_id: null,
          status: `error: ${(e as Error).message.slice(0, 100)}`,
        });
        abortedIds.add(step.id);
      }

      recs.shift();
      // D7: scoped recheck — re-compute plan from fresh health snapshot.
      // The next plan may drop completed steps and re-introduce failed
      // steps with bumped retry suffix (D1).
      if (recs.length === 0 || stepCount >= maxJobs) break;
      const freshHealth = await engine.getHealth();
      recs = computeRecommendations(freshHealth, ctx).filter((r) => r.status === 'remediable');
    }
  };

  let budgetExhaustedAt: InstanceType<typeof BudgetExhausted> | null = null;
  try {
    await withBudgetTracker(remediateTracker, runLoop);
  } catch (err) {
    if (err instanceof BudgetExhausted) {
      budgetExhaustedAt = err;
      console.error(
        `\n[remediate] BudgetExhausted (${err.reason}): spent $${err.spent.toFixed(4)} > cap $${err.cap.toFixed(2)}.\n` +
          `Checkpoint saved. Resume with:\n` +
          `  gbrain doctor --remediate --resume ${planHash}\n`,
      );
    } else {
      throw err;
    }
  }

  // Clear checkpoint on a clean run (no budget abort). Failed steps in the
  // submitted set don't disqualify the cleanup — they re-surface on the
  // next plan with bumped suffixes.
  if (!budgetExhaustedAt) {
    clearRemediationCheckpoint(planHash);
  }

  const finalHealth = await engine.getHealth();
  const result = {
    doctor_run_id: doctorRunId,
    brain_score_initial: initialHealth.brain_score,
    brain_score_final: finalHealth.brain_score,
    brain_score_target: targetScore,
    target_reached: finalHealth.brain_score >= targetScore,
    submitted,
    aborted_count: abortedIds.size,
  };

  if (jsonOutput) {
    console.log(JSON.stringify(result, null, 2));
  } else {
    console.log(`\nBrain score: ${initialHealth.brain_score} → ${finalHealth.brain_score} (target ${targetScore})`);
    console.log(`Submitted: ${submitted.length} job(s), ${abortedIds.size} aborted/failed`);
  }

  const anyFailed = submitted.some((s) => s.status !== 'completed' && s.status !== 'submitted');
  if (budgetExhaustedAt || anyFailed) process.exit(1);
}

/**
 * Build RecommendationContext from engine + config.
 * Pure read; no side effects.
 */
async function loadRecommendationContext(engine: BrainEngine) {
  // v0.37 fix wave (Lane E.4 + CDX2-11): read schema-sizing fields from
  // gateway, not DB. The DB plane is schema-applied metadata; the file
  // plane is the gateway runtime source. Pre-fix this context produced
  // stale recommendations on fresh installs whose DB rows hadn't been
  // populated.
  //
  // Also extended the API-key check to recognize the ZE key alongside
  // OpenAI (was OpenAI-only). After Lane C.3, zeroentropy_api_key lives
  // in GBrainConfig + propagates to the gateway env dict.
  const repoPath = await engine.getConfig('sync.repo_path');
  let embeddingModel: string | undefined;
  let embeddingDimensions: number | undefined;
  try {
    const gw = await import('../core/ai/gateway.ts');
    embeddingModel = gw.getEmbeddingModel();
    embeddingDimensions = gw.getEmbeddingDimensions();
  } catch {
    // Gateway unconfigured — fall back to DB plane as a best-effort hint
    // (preserves doctor running before any engine.connect()).
    const dbModel = await engine.getConfig('embedding_model');
    const dbDims = await engine.getConfig('embedding_dimensions');
    embeddingModel = dbModel ?? undefined;
    embeddingDimensions = dbDims ? Number(dbDims) : undefined;
  }
  // Provider-aware key check. The active embedding provider determines
  // which key matters. Pre-fix this was OpenAI-only, so a ZE brain with
  // OPENAI_API_KEY set looked "healthy" even though no key reached ZE.
  const { loadConfigFileOnly } = await import('../core/config.ts');
  const fileCfg = loadConfigFileOnly();
  let hasEmbeddingApiKey = false;
  if (embeddingModel?.startsWith('openai:')) {
    hasEmbeddingApiKey = !!(process.env.OPENAI_API_KEY || fileCfg?.openai_api_key);
  } else if (embeddingModel?.startsWith('zeroentropyai:')) {
    hasEmbeddingApiKey = !!(process.env.ZEROENTROPY_API_KEY || fileCfg?.zeroentropy_api_key);
  } else {
    // Voyage / generic openai-compatible / unknown provider — fall back
    // to "any key present" as the legacy hint.
    hasEmbeddingApiKey = !!(
      process.env.OPENAI_API_KEY ||
      process.env.ZEROENTROPY_API_KEY ||
      fileCfg?.openai_api_key ||
      fileCfg?.zeroentropy_api_key
    );
  }
  return {
    repoPath: repoPath ?? undefined,
    embeddingModel,
    embeddingDimensions,
    hasEmbeddingApiKey,
    hasChatApiKey: !!(process.env.ANTHROPIC_API_KEY || fileCfg?.anthropic_api_key),
  };
}

function parseIntFlag(args: string[], flag: string): number | null {
  const i = args.indexOf(flag);
  if (i === -1 || i === args.length - 1) return null;
  const v = parseInt(args[i + 1] ?? '', 10);
  return isNaN(v) ? null : v;
}

function parseFloatFlag(args: string[], flag: string): number | null {
  const i = args.indexOf(flag);
  if (i === -1 || i === args.length - 1) return null;
  const v = parseFloat(args[i + 1] ?? '');
  return isNaN(v) ? null : v;
}

// =================================================================
// v0.39 T7 + T9 — schema-pack doctor checks
// =================================================================
// Three checks per v0.38 CEO plan that never shipped at v0.38 time:
//   schema_pack_active       — does the active pack resolve cleanly?
//   schema_pack_consistency  — what % of pages match the active pack?
//   schema_pack_source_drift — do per-source packs disagree?
// All three are warn-only; never fail-block.

export async function checkSchemaPackActive(engine: BrainEngine): Promise<Check> {
  try {
    const { loadActivePack } = await import('../core/schema-pack/load-active.ts');
    const { loadConfig } = await import('../core/config.ts');
    const pack = await loadActivePack({ cfg: loadConfig(), remote: false });
    return {
      name: 'schema_pack_active',
      status: 'ok',
      message: `Active pack: ${pack.manifest.name} v${pack.manifest.version} (${pack.manifest.page_types.length} types, ${pack.manifest.link_types?.length ?? 0} link verbs)`,
    };
  } catch (e) {
    return {
      name: 'schema_pack_active',
      status: 'warn',
      message: `Active pack failed to resolve: ${(e as Error).message}. Run \`gbrain schema active\` to debug.`,
    };
  }
}

export async function checkSchemaPackConsistency(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ src: string; total: string | number; untyped: string | number }>(
      `SELECT
         source_id AS src,
         COUNT(*)::text AS total,
         COUNT(*) FILTER (WHERE type IS NULL OR type = '')::text AS untyped
       FROM pages
       WHERE deleted_at IS NULL
       GROUP BY source_id
       ORDER BY source_id`,
    );
    if (rows.length === 0) {
      return { name: 'schema_pack_consistency', status: 'ok', message: 'No pages in any source — schema consistency N/A.' };
    }
    let worstPct = 0;
    let worstSrc = '';
    let worstUntyped = 0;
    let worstTotal = 0;
    for (const r of rows) {
      const total = Number(r.total);
      const untyped = Number(r.untyped);
      if (total === 0) continue;
      const pct = untyped / total;
      if (pct > worstPct) {
        worstPct = pct;
        worstSrc = r.src;
        worstUntyped = untyped;
        worstTotal = total;
      }
    }
    if (worstPct === 0) {
      return { name: 'schema_pack_consistency', status: 'ok', message: 'All pages match the active schema pack across every source.' };
    }
    const pctStr = (worstPct * 100).toFixed(1);
    if (worstPct >= 0.1) {
      return {
        name: 'schema_pack_consistency',
        status: 'warn',
        message: `Source \`${worstSrc}\`: ${worstUntyped} of ${worstTotal} pages (${pctStr}%) have no type matching the active pack. Run \`gbrain schema detect --source ${worstSrc}\` to propose a pack matching your content shape.`,
      };
    }
    return {
      name: 'schema_pack_consistency',
      status: 'ok',
      message: `${pctStr}% untyped at worst (source \`${worstSrc}\`) — under the 10% warn threshold.`,
    };
  } catch (e) {
    return {
      name: 'schema_pack_consistency',
      status: 'ok',
      message: `Skipped: ${(e as Error).message}`,
    };
  }
}

export async function checkSchemaPackSourceDrift(engine: BrainEngine): Promise<Check> {
  try {
    // Compare per-source schema_pack overrides (tier 3 DB config) to detect
    // multi-source brains where different sources point at conflicting packs.
    const rows = await engine.executeRaw<{ key: string; value: string }>(
      `SELECT key, value FROM config WHERE key LIKE 'schema_pack.source.%'`,
    );
    if (rows.length === 0) {
      return { name: 'schema_pack_source_drift', status: 'ok', message: 'No per-source pack overrides — drift N/A.' };
    }
    const distinctPacks = new Set(rows.map((r) => r.value).filter(Boolean));
    if (distinctPacks.size <= 1) {
      return { name: 'schema_pack_source_drift', status: 'ok', message: `${rows.length} per-source overrides; all point at the same pack.` };
    }
    return {
      name: 'schema_pack_source_drift',
      status: 'warn',
      message: `Per-source pack divergence detected: ${distinctPacks.size} distinct packs across ${rows.length} sources. Run \`gbrain sources list\` then \`gbrain schema active --source <id>\` per source to audit.`,
    };
  } catch (e) {
    return {
      name: 'schema_pack_source_drift',
      status: 'ok',
      message: `Skipped: ${(e as Error).message}`,
    };
  }
}
