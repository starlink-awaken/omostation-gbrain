/**
 * AI Gateway chat capability — split out of gateway.ts (BET-Y1Q3-T6-04).
 * Chat + tool-loop orchestration. State is shared through gateway.ts
 * accessors to avoid a circular import.
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { generateObject, generateText } from 'ai';
import { createOpenAI } from '@ai-sdk/openai';
import { createGoogleGenerativeAI } from '@ai-sdk/google';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createOpenAICompatible } from '@ai-sdk/openai-compatible';
import { z } from 'zod';
import type {
  AIGatewayConfig,
  Recipe,
  TouchpointKind,
} from './types.ts';
import { resolveRecipe, assertTouchpoint } from './model-resolver.ts';
import { resolveModel, TIER_DEFAULTS } from '../model-config.ts';
import {
  AIConfigError,
  AITransientError,
  normalizeAIError,
} from './errors.ts';
import {
  BudgetTracker,
  extractUsageFromError as _extractUsageFromError,
  type BudgetKind,
} from '../budget/budget-tracker.ts';
import {
  getChatModel,
  getExtendedModelsForProvider,
  requireConfig,
  applyOpenAICompatConfig,
  applyResolveAuth,
  _getBudgetStore,
  _getChatTransport,
  _getModelCache,
  rerank,
} from './gateway.ts';

function estimateChatInputTokens(opts: { system?: string; messages?: Array<{ content?: unknown }> }): number {
  let chars = (opts.system ?? '').length;
  for (const m of opts.messages ?? []) {
    if (typeof m.content === 'string') chars += m.content.length;
    else if (Array.isArray(m.content)) {
      for (const block of m.content) {
        const t = (block as { text?: unknown }).text;
        if (typeof t === 'string') chars += t.length;
      }
    }
  }
  return Math.ceil(chars / 4);
}

// ---- Chat (commit 1) ----

/**
 * Provider-neutral message shape stored in subagent persistence (commit 2a).
 * Vercel AI SDK's `generateText` accepts this directly via its `messages`
 * parameter; tool-use blocks are normalized across providers.
 */
export type ChatRole = 'system' | 'user' | 'assistant' | 'tool';

export type ChatBlock =
  | { type: 'text'; text: string }
  | { type: 'tool-call'; toolCallId: string; toolName: string; input: unknown }
  | { type: 'tool-result'; toolCallId: string; toolName: string; output: unknown; isError?: boolean };

export interface ChatMessage {
  role: ChatRole;
  content: string | ChatBlock[];
}

export interface ChatToolDef {
  name: string;
  description: string;
  /** JSON Schema for tool input. */
  inputSchema: Record<string, unknown>;
}

export interface ChatResult {
  /** Final text content concatenated from text blocks. */
  text: string;
  /** Raw assistant response blocks (text + tool-call entries) for persistence. */
  blocks: ChatBlock[];
  /** Reason the model stopped. Provider-neutral mapping of stop_reason / finish_reason. */
  stopReason: 'end' | 'tool_calls' | 'length' | 'refusal' | 'content_filter' | 'other';
  /** Provider-neutral usage. cache_* are present only when the active provider returned them (Anthropic). */
  usage: {
    input_tokens: number;
    output_tokens: number;
    cache_read_tokens: number;
    cache_creation_tokens: number;
  };
  /** "provider:modelId" string of the model that actually answered. */
  model: string;
  /** Recipe id for the answering provider. */
  providerId: string;
  /** Raw provider metadata (Anthropic-specific cache fields, OpenAI finish_reason, etc.) for downstream callers that need it. */
  providerMetadata?: Record<string, any>;
}

export interface ChatOpts {
  /** "provider:modelId" — defaults to config.chat_model. */
  model?: string;
  /** System prompt. */
  system?: string;
  messages: ChatMessage[];
  tools?: ChatToolDef[];
  maxTokens?: number;
  abortSignal?: AbortSignal;
  /**
   * Anthropic-specific: cache the system prompt + last tool def. Silently
   * ignored on providers without `supports_prompt_cache`.
   */
  cacheSystem?: boolean;
}

async function resolveChatProvider(modelStr: string): Promise<{ model: any; recipe: Recipe; modelId: string }> {
  const { parsed, recipe } = resolveRecipe(modelStr);
  assertTouchpoint(recipe, 'chat', parsed.modelId, getExtendedModelsForProvider(parsed.providerId));
  const cfg = requireConfig();

  const cacheKey = `chat:${recipe.id}:${parsed.modelId}:${cfg.base_urls?.[recipe.id] ?? ''}`;
  const cached = _getModelCache().get(cacheKey);
  if (cached) return { model: cached, recipe, modelId: parsed.modelId };

  const model = instantiateChat(recipe, parsed.modelId, cfg);
  _getModelCache().set(cacheKey, model);
  return { model, recipe, modelId: parsed.modelId };
}

function instantiateChat(recipe: Recipe, modelId: string, cfg: AIGatewayConfig): any {
  switch (recipe.implementation) {
    case 'native-openai': {
      const apiKey = cfg.env.OPENAI_API_KEY;
      if (!apiKey) throw new AIConfigError(`OpenAI chat requires OPENAI_API_KEY.`, recipe.setup_hint);
      return createOpenAI({ apiKey }).languageModel(modelId);
    }
    case 'native-google': {
      const apiKey = cfg.env.GOOGLE_GENERATIVE_AI_API_KEY;
      if (!apiKey) throw new AIConfigError(`Google chat requires GOOGLE_GENERATIVE_AI_API_KEY.`, recipe.setup_hint);
      return createGoogleGenerativeAI({ apiKey }).languageModel(modelId);
    }
    case 'native-anthropic': {
      const apiKey = cfg.env.ANTHROPIC_API_KEY;
      if (!apiKey) throw new AIConfigError(`Anthropic chat requires ANTHROPIC_API_KEY.`, recipe.setup_hint);
      return createAnthropic({ apiKey }).languageModel(modelId);
    }
    case 'openai-compatible': {
      // D12=A: unified auth via Recipe.resolveAuth (or default).
      const auth = applyResolveAuth(recipe, cfg, 'chat');
      // v0.32: env-templated base URL + optional fetch wrapper.
      const compat = applyOpenAICompatConfig(recipe, cfg);
      return createOpenAICompatible({
        name: recipe.id,
        baseURL: compat.baseURL,
        ...(compat.fetch ? { fetch: compat.fetch } : {}),
        ...auth,
      }).languageModel(modelId);
    }
    default:
      throw new AIConfigError(`Unknown implementation: ${(recipe as any).implementation}`);
  }
}

/**
 * Map AI SDK's `finish_reason` (and provider-specific signals) to a provider-
 * neutral `stopReason`. This is the structural-signal layer that
 * `chatWithFallback` (commit 3) consults BEFORE any regex heuristic (per D8).
 */
function mapStopReason(
  finishReason: string | undefined,
  providerMetadata: Record<string, any> | undefined,
): ChatResult['stopReason'] {
  // Anthropic: `stop_reason: 'refusal'` lands in providerMetadata.anthropic.
  const anthropicStop = providerMetadata?.anthropic?.stopReason ?? providerMetadata?.anthropic?.stop_reason;
  if (anthropicStop === 'refusal') return 'refusal';
  // OpenAI: `finish_reason: 'content_filter'`.
  if (finishReason === 'content-filter' || finishReason === 'content_filter') return 'content_filter';
  if (finishReason === 'tool-calls' || finishReason === 'tool_calls') return 'tool_calls';
  if (finishReason === 'length' || finishReason === 'max-tokens') return 'length';
  if (finishReason === 'stop' || finishReason === 'end' || finishReason === 'end-turn') return 'end';
  return 'other';
}

/**
 * Run one chat completion turn. Provider-neutral wrapper over Vercel AI SDK's
 * `generateText`. Tool-use blocks are normalized; cache_control markers are
 * applied only on Anthropic when `cacheSystem: true`.
 *
 * Crash-resumable replay is the caller's responsibility (subagent.ts persists
 * blocks via the provider-neutral schema landing in commit 2a).
 */
export async function chat(opts: ChatOpts): Promise<ChatResult> {
  const tracker = _getBudgetStore().getStore() ?? null;
  const modelStrEarly = opts.model ?? getChatModel();
  const estimatedInputTokens = estimateChatInputTokens(opts);
  const maxOutputTokens = opts.maxTokens ?? 4096;

  // TX5: reserve BEFORE the provider call. Throws BudgetExhausted on cost,
  // runtime, or no_pricing (when cap is set). Pre-resolution model id is
  // fine here — resolveChatProvider would map aliases the same way for the
  // cost lookup. record() below uses the real result.model.
  if (tracker) {
    tracker.reserve({
      modelId: modelStrEarly,
      estimatedInputTokens,
      maxOutputTokens,
      kind: 'chat' as BudgetKind,
      label: 'gateway.chat',
    });
  }

  // Test seam: when a test transport is installed, route through it without
  // touching provider resolution, AI SDK, or any network. See
  // __setChatTransportForTests. Production paths see _getChatTransport() === null.
  const testTransport = _getChatTransport();
  if (testTransport) {
    let res: ChatResult | null = null;
    let threw: unknown = null;
    try {
      res = await testTransport(opts);
      return res;
    } catch (err) {
      threw = err;
      throw err;
    } finally {
      if (tracker) {
        try {
          if (res) {
            tracker.record({
              modelId: res.model ?? modelStrEarly,
              inputTokens: res.usage.input_tokens,
              outputTokens: res.usage.output_tokens,
              label: 'gateway.chat',
            });
          } else {
            const usage = _extractUsageFromError(threw, {
              inputTokens: estimatedInputTokens,
              outputTokens: maxOutputTokens,
            });
            tracker.record({
              modelId: modelStrEarly,
              inputTokens: usage.inputTokens,
              outputTokens: usage.outputTokens,
              label: 'gateway.chat',
            });
          }
        } catch {
          // record() can throw BudgetExhausted (TX1) — suppress here so the
          // original error (if any) wins; the BudgetExhausted is surfaced
          // on the NEXT call via reserve(). For test transport this branch
          // is rare in practice.
        }
      }
    }
  }

  const modelStr = modelStrEarly;
  const { model, recipe, modelId } = await resolveChatProvider(modelStr);

  const supportsCache = recipe.touchpoints.chat?.supports_prompt_cache === true;
  const useCache = !!opts.cacheSystem && supportsCache;

  // Build messages. Anthropic prompt-cache markers ride on system + last tool
  // via providerOptions; the AI SDK accepts the system as a string for
  // generateText, so cache markers go through providerOptions.anthropic.
  const tools = (opts.tools ?? []).reduce((acc, t) => {
    acc[t.name] = {
      description: t.description,
      inputSchema: { jsonSchema: t.inputSchema } as any,
    };
    return acc;
  }, {} as Record<string, any>);

  const providerOptions: Record<string, any> = {};
  if (useCache) {
    providerOptions.anthropic = { cacheControl: { type: 'ephemeral' } };
  }

  let _budgetRecorded = false;
  const _recordBudget = (modelLabel: string, inputTokens: number, outputTokens: number): void => {
    if (!tracker || _budgetRecorded) return;
    _budgetRecorded = true;
    try {
      tracker.record({
        modelId: modelLabel,
        inputTokens,
        outputTokens,
        label: 'gateway.chat',
      });
    } catch {
      // BudgetExhausted (TX1) raised here; surface via next reserve()
    }
  };

  try {
    const result = await generateText({
      model,
      system: opts.system,
      messages: opts.messages as any,
      tools: opts.tools && opts.tools.length > 0 ? tools : undefined,
      maxOutputTokens: opts.maxTokens ?? 4096,
      abortSignal: opts.abortSignal,
      providerOptions: Object.keys(providerOptions).length > 0 ? providerOptions : undefined,
    });

    // Normalize blocks. Vercel SDK gives us `result.content` (an array of typed
    // parts) for v6+; fall back to text + toolCalls for older shapes.
    const blocks: ChatBlock[] = [];
    const rawContent: any[] = (result as any).content ?? [];
    if (Array.isArray(rawContent) && rawContent.length > 0) {
      for (const part of rawContent) {
        if (part.type === 'text') blocks.push({ type: 'text', text: part.text });
        else if (part.type === 'tool-call') {
          blocks.push({
            type: 'tool-call',
            toolCallId: part.toolCallId,
            toolName: part.toolName,
            input: part.input ?? part.args,
          });
        }
      }
    } else {
      // Fallback shape for SDK versions exposing flat .text and .toolCalls.
      if (typeof (result as any).text === 'string' && (result as any).text.length > 0) {
        blocks.push({ type: 'text', text: (result as any).text });
      }
      for (const tc of (result as any).toolCalls ?? []) {
        blocks.push({
          type: 'tool-call',
          toolCallId: tc.toolCallId,
          toolName: tc.toolName,
          input: tc.input ?? tc.args,
        });
      }
    }

    const usage = (result as any).usage ?? {};
    const providerMetadata = (result as any).providerMetadata as Record<string, any> | undefined;
    const anthropicCache = providerMetadata?.anthropic ?? {};

    const inTok = Number(usage.inputTokens ?? usage.promptTokens ?? 0);
    const outTok = Number(usage.outputTokens ?? usage.completionTokens ?? 0);
    _recordBudget(`${recipe.id}:${modelId}`, inTok, outTok);

    return {
      text: blocks.filter(b => b.type === 'text').map(b => (b as { type: 'text'; text: string }).text).join(''),
      blocks,
      stopReason: mapStopReason((result as any).finishReason, providerMetadata),
      usage: {
        input_tokens: inTok,
        output_tokens: outTok,
        cache_read_tokens: Number(anthropicCache.cacheReadInputTokens ?? anthropicCache.cache_read_input_tokens ?? 0),
        cache_creation_tokens: Number(anthropicCache.cacheCreationInputTokens ?? anthropicCache.cache_creation_input_tokens ?? 0),
      },
      model: `${recipe.id}:${modelId}`,
      providerId: recipe.id,
      providerMetadata,
    };
  } catch (err) {
    // Pessimistic fallback (A3 amended): when err.usage isn't there, charge
    // the worst-case ceiling — better to overcount on failure than under.
    const fallback = _extractUsageFromError(err, {
      inputTokens: estimatedInputTokens,
      outputTokens: maxOutputTokens,
    });
    _recordBudget(`${recipe.id}:${modelId}`, fallback.inputTokens, fallback.outputTokens);
    throw normalizeAIError(err, `chat(${recipe.id}:${modelId})`);
  }
}

// ---- Tool loop (v0.38 — D11 + D6/D7 gateway-native subagent path) ----

/**
 * A tool handler runs a single tool invocation. `idempotent` lets the loop
 * safely re-execute a pending row on crash-replay; non-idempotent tools that
 * crashed mid-execute are surfaced as a hard error.
 */
export interface ToolHandler {
  idempotent?: boolean;
  execute(input: unknown, signal: AbortSignal): Promise<unknown>;
}

/**
 * State the caller carries in from a prior crashed run. The reconciler keys
 * by gbrain-owned `gbrainToolUseId` (D11), NOT provider-supplied IDs.
 * `priorMessages` is the chat history up to the assistant's last turn;
 * `priorTools` maps gbrainToolUseId → outcome. The D5 read-time shim
 * synthesizes gbrainToolUseIds for legacy v1 rows so this Map sees both
 * shapes uniformly.
 */
export interface ToolLoopReplayState {
  priorMessages: ChatMessage[];
  priorTools: Map<string, { status: 'pending' | 'complete' | 'failed'; output?: unknown; error?: string }>;
  nextTurnIdx: number;
  nextMessageIdx: number;
}

export interface ToolLoopOpts {
  /** "provider:modelId" — defaults to config.chat_model. */
  model?: string;
  /** System prompt (provider-neutral). Cached when caching supported + cacheSystem true. */
  system?: string;
  /**
   * Initial user message(s). When `replayState` is set, these are prepended only
   * if `replayState.priorMessages` is empty — typically empty on a fresh call,
   * non-empty on a fresh-from-scratch run.
   */
  initialMessages: ChatMessage[];
  /** Tool definitions (provider-neutral JSON Schema). */
  tools: ChatToolDef[];
  /** Implementations keyed by tool name. */
  toolHandlers: Map<string, ToolHandler>;
  /** Hard cap on loop iterations. Default 20. */
  maxTurns?: number;
  /** Per-turn max output tokens. Default 4096. */
  maxTokens?: number;
  abortSignal?: AbortSignal;
  /** Apply Anthropic cache_control to system + last tool. Silently ignored elsewhere. */
  cacheSystem?: boolean;

  /** Crash-replay state. When set, the loop resumes from the recorded position. */
  replayState?: ToolLoopReplayState;

  /**
   * D11 + write-ordering invariant callbacks. Fire BEFORE side effects so a
   * crash mid-execute is reconcilable on the next replay.
   *
   * Ordering per turn:
   *   1. onAssistantTurn  — assistant message persisted (D11 step 1)
   *   2. onToolCallStart   — pending row persisted (D11 step 2)
   *   3. handler.execute   — side effect
   *   4. onToolCallComplete / onToolCallFailed (D11 step 4)
   */
  onAssistantTurn?: (turnIdx: number, messageIdx: number, blocks: ChatBlock[], usage: ChatResult['usage'], model: string) => Promise<void>;
  /**
   * Persist a pending tool execution. The caller assigns ordinal + uuid v7 and
   * returns them so the loop can key replay by gbrainToolUseId. The provider
   * supplies its own `providerToolCallId` (kept as a debug-only side channel).
   */
  onToolCallStart?: (
    turnIdx: number,
    messageIdx: number,
    ordinal: number,
    toolName: string,
    input: unknown,
    providerToolCallId: string,
  ) => Promise<{ gbrainToolUseId: string }>;
  onToolCallComplete?: (gbrainToolUseId: string, output: unknown) => Promise<void>;
  onToolCallFailed?: (gbrainToolUseId: string, error: string) => Promise<void>;

  /** Optional per-call heartbeat for observability. */
  onHeartbeat?: (event: string, data: Record<string, unknown>) => void;
}

export type ToolLoopStopReason = 'end' | 'max_turns' | 'refusal' | 'content_filter' | 'aborted' | 'unrecoverable';

export interface ToolLoopResult {
  finalText: string;
  totalTurns: number;
  totalUsage: ChatResult['usage'];
  stopReason: ToolLoopStopReason;
  /** Final messages array including all assistant + tool results. Caller persists if desired. */
  messages: ChatMessage[];
}

/**
 * Provider-agnostic tool-calling loop. Wraps `gateway.chat()` with:
 *   - assistant→tool-dispatch→tool-result cycle
 *   - gbrain-stable IDs (D11) at first observation
 *   - write-ordering invariant (persist before side effect)
 *   - crash-replay reconciliation via gbrainToolUseId
 *   - capability-driven cache_control (Anthropic only)
 *
 * This replaces the direct `new Anthropic()` + `client.create()` path in
 * `src/core/minions/handlers/subagent.ts`. The provider abstraction lives in
 * `gateway.chat()` (Vercel AI SDK); this function is just the loop control.
 *
 * Designed so the caller (subagent handler) supplies persistence callbacks —
 * the loop itself is stateless beyond `replayState`. That keeps it testable
 * via `__setChatTransportForTests` without any DB.
 */
export async function toolLoop(opts: ToolLoopOpts): Promise<ToolLoopResult> {
  const maxTurns = opts.maxTurns ?? 20;
  const maxTokens = opts.maxTokens ?? 4096;
  const handlers = opts.toolHandlers;
  const totalUsage: ChatResult['usage'] = {
    input_tokens: 0,
    output_tokens: 0,
    cache_read_tokens: 0,
    cache_creation_tokens: 0,
  };

  // Seed messages: prior history (replay) or initial.
  const messages: ChatMessage[] = opts.replayState
    ? [...opts.replayState.priorMessages]
    : [...opts.initialMessages];
  if (opts.replayState && opts.replayState.priorMessages.length === 0) {
    messages.push(...opts.initialMessages);
  }
  let turnIdx = opts.replayState?.nextTurnIdx ?? 0;
  let messageIdx = opts.replayState?.nextMessageIdx ?? 0;
  let finalText = '';
  let stopReason: ToolLoopStopReason = 'end';

  while (turnIdx < maxTurns) {
    if (opts.abortSignal?.aborted) {
      stopReason = 'aborted';
      break;
    }

    opts.onHeartbeat?.('turn_start', { turn_idx: turnIdx });

    let chatResult: ChatResult;
    try {
      chatResult = await chat({
        model: opts.model,
        system: opts.system,
        messages,
        tools: opts.tools,
        maxTokens,
        abortSignal: opts.abortSignal,
        cacheSystem: opts.cacheSystem,
      });
    } catch (err) {
      opts.onHeartbeat?.('llm_call_failed', {
        turn_idx: turnIdx,
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }

    totalUsage.input_tokens += chatResult.usage.input_tokens;
    totalUsage.output_tokens += chatResult.usage.output_tokens;
    totalUsage.cache_read_tokens += chatResult.usage.cache_read_tokens;
    totalUsage.cache_creation_tokens += chatResult.usage.cache_creation_tokens;

    // D11 step 1: persist assistant turn BEFORE any tool dispatch.
    const assistantMessageIdx = messageIdx++;
    await opts.onAssistantTurn?.(turnIdx, assistantMessageIdx, chatResult.blocks, chatResult.usage, chatResult.model);
    messages.push({ role: 'assistant', content: chatResult.blocks });

    // Check stop reason BEFORE tool dispatch. The loop only continues on tool_calls.
    if (chatResult.stopReason === 'refusal') {
      stopReason = 'refusal';
      finalText = chatResult.text;
      break;
    }
    if (chatResult.stopReason === 'content_filter') {
      stopReason = 'content_filter';
      finalText = chatResult.text;
      break;
    }

    const toolCalls = chatResult.blocks.filter(
      (b): b is { type: 'tool-call'; toolCallId: string; toolName: string; input: unknown } =>
        b.type === 'tool-call',
    );

    if (toolCalls.length === 0) {
      stopReason = 'end';
      finalText = chatResult.text;
      break;
    }

    // D11 + write-ordering invariant: persist pending → execute → settle.
    const toolResultBlocks: ChatBlock[] = [];
    for (let callIdx = 0; callIdx < toolCalls.length; callIdx++) {
      const call = toolCalls[callIdx];
      if (opts.abortSignal?.aborted) {
        stopReason = 'aborted';
        break;
      }

      const handler = handlers.get(call.toolName);
      if (!handler) {
        // Tool not registered. Synthesize an error result; don't persist.
        toolResultBlocks.push({
          type: 'tool-result',
          toolCallId: call.toolCallId,
          toolName: call.toolName,
          output: `tool "${call.toolName}" is not in the registry for this subagent`,
          isError: true,
        });
        opts.onHeartbeat?.('tool_failed', { turn_idx: turnIdx, tool_name: call.toolName, error: 'not_registered' });
        continue;
      }

      // Step 2: persist pending row + claim gbrainToolUseId. The caller's
      // callback handles uniqueness contention via ON CONFLICT DO NOTHING +
      // re-read pattern (see persistToolExecPending in subagent.ts).
      const { gbrainToolUseId } = (await opts.onToolCallStart?.(
        turnIdx,
        assistantMessageIdx,
        callIdx,
        call.toolName,
        call.input,
        call.toolCallId,
      )) ?? { gbrainToolUseId: `inline-${turnIdx}-${callIdx}` };

      // Replay short-circuit: prior outcome wins, idempotent re-execute allowed.
      const prior = opts.replayState?.priorTools.get(gbrainToolUseId);
      if (prior?.status === 'complete') {
        toolResultBlocks.push({
          type: 'tool-result',
          toolCallId: call.toolCallId,
          toolName: call.toolName,
          output: prior.output,
        });
        opts.onHeartbeat?.('tool_replay_complete', { turn_idx: turnIdx, tool_name: call.toolName });
        continue;
      }
      if (prior?.status === 'failed') {
        toolResultBlocks.push({
          type: 'tool-result',
          toolCallId: call.toolCallId,
          toolName: call.toolName,
          output: prior.error ?? 'tool failed',
          isError: true,
        });
        opts.onHeartbeat?.('tool_replay_failed', { turn_idx: turnIdx, tool_name: call.toolName });
        continue;
      }
      if (prior?.status === 'pending' && !handler.idempotent) {
        // Non-idempotent crash-mid-execute. Surface as unrecoverable.
        stopReason = 'unrecoverable';
        throw new Error(
          `non-idempotent tool "${call.toolName}" pending on resume; gbrainToolUseId=${gbrainToolUseId} — cannot safely re-run`,
        );
      }

      // Step 3: execute (side effect).
      opts.onHeartbeat?.('tool_called', { turn_idx: turnIdx, tool_name: call.toolName });
      try {
        const output = await handler.execute(call.input, opts.abortSignal ?? new AbortController().signal);
        // Step 4: settle complete.
        await opts.onToolCallComplete?.(gbrainToolUseId, output);
        toolResultBlocks.push({
          type: 'tool-result',
          toolCallId: call.toolCallId,
          toolName: call.toolName,
          output,
        });
        opts.onHeartbeat?.('tool_result', { turn_idx: turnIdx, tool_name: call.toolName });
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : String(err);
        await opts.onToolCallFailed?.(gbrainToolUseId, errMsg);
        toolResultBlocks.push({
          type: 'tool-result',
          toolCallId: call.toolCallId,
          toolName: call.toolName,
          output: errMsg,
          isError: true,
        });
        opts.onHeartbeat?.('tool_failed', { turn_idx: turnIdx, tool_name: call.toolName, error: errMsg });
      }
    }

    if (stopReason === 'aborted') break;

    // Feed all tool results back as a single user message.
    const userMessageIdx = messageIdx++;
    void userMessageIdx;
    messages.push({ role: 'user', content: toolResultBlocks });

    turnIdx++;
  }

  if (turnIdx >= maxTurns && stopReason === 'end') {
    stopReason = 'max_turns';
  }

  return { finalText, totalTurns: turnIdx, totalUsage, stopReason, messages };
}
