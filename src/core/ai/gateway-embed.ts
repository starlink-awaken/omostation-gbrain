/**
 * AI Gateway embed capability — split out of gateway.ts (BET-Y1Q3-T6-04).
 * Embedding, multimodal embedding, provider shims, and shrink-on-miss
 * batching. State is shared through gateway.ts accessors to avoid a
 * circular import.
 */
import { embedMany, generateObject, generateText } from 'ai';
import { createOpenAI } from '@ai-sdk/openai';
import { createGoogleGenerativeAI } from '@ai-sdk/google';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createOpenAICompatible } from '@ai-sdk/openai-compatible';
import { z } from 'zod';
import type {
  AIGatewayConfig,
  EmbedMultimodalOpts,
  MultimodalBatchResult,
  MultimodalInput,
  Recipe,
  TouchpointKind,
} from './types.ts';
import { resolveRecipe, assertTouchpoint } from './model-resolver.ts';
import { resolveModel, TIER_DEFAULTS } from '../model-config.ts';
import { dimsProviderOptions } from './dims.ts';
import { AIConfigError, AITransientError, normalizeAIError } from './errors.ts';
import { DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_DIMENSIONS } from './defaults.ts';
import {
  MAX_CHARS,
  DEFAULT_CHARS_PER_TOKEN,
  SHRINK_FLOOR,
  SHRINK_HEAL_AFTER,
  type ShrinkEntry,
  applyOpenAICompatConfig,
  applyResolveAuth,
  defaultResolveAuth,
  getEmbeddingModel,
  getExtendedModelsForProvider,
  requireConfig,
  _getConfig,
  _getModelCache,
  _getShrinkState,
  _getEmbedTransport,
  _getBudgetStore,
} from './gateway.ts';


// v0.31.8 (D2 + D10): hard ceiling on Voyage response size, sized as
// "unambiguously not a real Voyage response" rather than tight against
// typical batches. voyage-3-large × 16K embeddings ≈ 200 MB raw (3072
// dims × 4 bytes × 16K), which fits within this cap. Anything larger is
// unambiguously not legitimate. Layer 1 (Content-Length pre-check) and
// Layer 2 (per-embedding base64 cap) both compare against this constant.
export const MAX_VOYAGE_RESPONSE_BYTES = 256 * 1024 * 1024;

/**
 * v0.35.0.0+: same defense pattern as Voyage's cap but tagged separately so the
 * `instanceof` rethrow inside zeroEntropyCompatFetch only matches its own
 * throws (avoids cross-recipe entanglement if both shims fire in the same
 * process). Plan called for unifying these into one
 * `EmbeddingResponseTooLargeError` class — descoped because
 * `test/voyage-response-cap.test.ts` does structural source-text greps
 * pinning the Voyage name. Unification is a follow-up cleanup.
 */
export const MAX_ZEROENTROPY_RESPONSE_BYTES = 256 * 1024 * 1024;

/** Default safety factor when a recipe omits it. */
export const DEFAULT_SAFETY_FACTOR = 0.8;

export class VoyageResponseTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'VoyageResponseTooLargeError';
  }
}

export class ZeroEntropyResponseTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ZeroEntropyResponseTooLargeError';
  }
}


// ---- Embedding ----

/**
 * Voyage AI compatibility shim. Voyage's `/v1/embeddings` endpoint is OpenAI-shaped
 * but diverges on two parameters:
 *   - `encoding_format` only accepts `'base64'` (the AI SDK sends `'float'` by default,
 *     which makes Voyage respond with HTTP 400). Force `'base64'` so the SDK round-trip
 *     parses correctly.
 *   - OpenAI's `dimensions` parameter is rejected; Voyage uses `output_dimension`.
 *     Translate the field name when the caller explicitly requested a dimension.
 *
 * The mutated body is what gets sent on the wire; the AI SDK still receives a
 * base64-encoded response and decodes it as expected.
 */
// Cast through `unknown` because Bun's `typeof fetch` extends the standard
// signature with a `preconnect` method that arrow functions can't provide.
// The AI SDK only invokes the call signature; the Bun extension is irrelevant
// here. Without this cast, `tsc --noEmit` fails:
//   error TS2741: Property 'preconnect' is missing in type
//   '(input: RequestInfo | URL, init: RequestInit | ...) => Promise<Response>'
//   but required in type 'typeof fetch'.
const voyageCompatFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  // OUTBOUND: rewrite request body for Voyage's actual API contract.
  if (init?.body && typeof init.body === 'string') {
    try {
      const parsed = JSON.parse(init.body);
      if (parsed && typeof parsed === 'object') {
        let mutated = false;
        // Voyage rejects 'float' (the SDK default). Force the value Voyage accepts.
        if (parsed.encoding_format !== 'base64') {
          parsed.encoding_format = 'base64';
          mutated = true;
        }
        // Translate OpenAI's `dimensions` to Voyage's `output_dimension`.
        if ('dimensions' in parsed) {
          const dims = parsed.dimensions;
          delete parsed.dimensions;
          if (typeof dims === 'number') parsed.output_dimension = dims;
          mutated = true;
        }
        if (mutated) {
          const newBody = JSON.stringify(parsed);
          // Drop Content-Length so fetch recomputes from the new body.
          const headers = new Headers(init.headers ?? {});
          headers.delete('content-length');
          init = { ...init, body: newBody, headers };
        }
      }
    } catch {
      // Body wasn't JSON — pass through untouched.
    }
  }

  const resp = await fetch(input, init);
  if (!resp.ok) return resp;
  const ct = resp.headers.get('content-type') ?? '';
  if (!ct.toLowerCase().includes('application/json')) return resp;

  // v0.31.8 (D2 + D10): Layer 1 — Content-Length pre-check BEFORE the
  // body is parsed. The pre-fix code did `await resp.clone().json()`
  // first, which fully parses arbitrary-size JSON into JS heap before
  // any size check could fire. A compromised/malicious Voyage endpoint
  // could OOM the worker on a single response. The 256 MB cap is sized
  // as "unambiguously not a real Voyage response" — voyage-3-large at
  // 3072 dims × 4 bytes × 16K embeddings (the plausible upper bound on
  // realistic load) decodes to ~200 MB raw and fits. Anything bigger
  // is unambiguously not legitimate.
  //
  // When Content-Length is missing (chunked transfer encoding), we
  // proceed and rely on Layer 2 (per-embedding base64 length check)
  // for OOM defense.
  const contentLengthHeader = resp.headers.get('content-length');
  if (contentLengthHeader) {
    const len = parseInt(contentLengthHeader, 10);
    if (Number.isFinite(len) && len > MAX_VOYAGE_RESPONSE_BYTES) {
      throw new VoyageResponseTooLargeError(
        `Voyage response Content-Length=${len} exceeds ${MAX_VOYAGE_RESPONSE_BYTES} bytes — ` +
        `likely compromised endpoint or misconfiguration`,
      );
    }
  }

  // INBOUND: rewrite response so the AI SDK's Zod schema validates.
  // Voyage diverges from OpenAI in two places that break the parser:
  //   - `embedding` is a base64 string (SDK schema expects `number[]`)
  //   - `usage` lacks `prompt_tokens` (SDK schema requires it when usage present)
  try {
    const json: any = await resp.clone().json();
    if (!json || typeof json !== 'object') return resp;
    let modified = false;
    if (Array.isArray(json.data)) {
      for (const item of json.data) {
        if (item && typeof item.embedding === 'string') {
          // v0.31.8 (D10 Layer 2): per-embedding cap. Catches the rare
          // case where Layer 1 was skipped (no Content-Length on chunked
          // encoding) AND a single embedding string is unreasonably large.
          // Estimate decoded size as 0.75 × base64 length (the canonical
          // base64 → bytes ratio).
          const estDecoded = Math.ceil(item.embedding.length * 0.75);
          if (estDecoded > MAX_VOYAGE_RESPONSE_BYTES) {
            throw new VoyageResponseTooLargeError(
              `Voyage embedding base64 exceeds ${MAX_VOYAGE_RESPONSE_BYTES} bytes ` +
              `(estimated ${estDecoded} bytes from ${item.embedding.length} base64 chars)`,
            );
          }
          // Voyage returns Float32 little-endian base64.
          const bytes = Buffer.from(item.embedding, 'base64');
          const floats = new Float32Array(
            bytes.buffer,
            bytes.byteOffset,
            Math.floor(bytes.byteLength / 4),
          );
          item.embedding = Array.from(floats);
          modified = true;
        }
      }
    }
    if (json.usage && typeof json.usage === 'object' && json.usage.prompt_tokens === undefined) {
      json.usage.prompt_tokens = typeof json.usage.total_tokens === 'number'
        ? json.usage.total_tokens
        : 0;
      modified = true;
    }
    if (!modified) return resp;
    return new Response(JSON.stringify(json), {
      status: resp.status,
      statusText: resp.statusText,
      headers: resp.headers,
    });
  } catch (err) {
    // OOM-cap throws MUST propagate. The catch is here for "Voyage returned
    // JSON I can't reshape" (parse error, unexpected schema) — falling back
    // to the original response is correct in that case. Letting the
    // too-large response through here would defeat the entire purpose of
    // Layer 2 (the per-embedding cap that fires when Content-Length wasn't
    // available to Layer 1).
    if (err instanceof VoyageResponseTooLargeError) throw err;
    // If parsing/transformation fails, fall back to the original response.
    return resp;
  }
}) as unknown as typeof fetch;

/**
 * ZeroEntropy compatibility shim. ZE's `/v1/models/embed` endpoint is NOT
 * OpenAI-compatible at the wire level:
 *  - Path: AI SDK adapter calls `${base_url}/embeddings`; ZE wants
 *    `${base_url}/models/embed`. Rewrite the URL path.
 *  - Body: inject `input_type: 'document'` (or `'query'` when threaded via
 *    providerOptions.openaiCompatible.input_type) and `encoding_format:
 *    'float'` (don't trust SDK default; strip any base64 caller injected
 *    to keep the response rewriter simple).
 *  - Response: ZE returns `{results: [{embedding: float[]}], usage:
 *    {total_bytes, total_tokens}}`. AI SDK's openai-compatible Zod schema
 *    expects `{data: [{embedding, index}], usage: {prompt_tokens, ...}}`.
 *    Rewrite both shapes.
 *
 * Layer 1 / Layer 2 OOM caps mirror the Voyage pattern; ZE embeddings are
 * float[] (not base64), so the Layer 2 cap compares against the JSON
 * payload size of each embedding rather than a base64 string length.
 */
const zeroEntropyCompatFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  // OUTBOUND: normalize URL, rewrite path /embeddings → /models/embed, then
  // rewrite body. fetch accepts RequestInfo (string | Request) | URL; we
  // handle all three so a `new Request(...)`-shaped caller works.
  let urlString: string;
  let baseInit: RequestInit = init ?? {};
  if (typeof input === 'string') {
    urlString = input;
  } else if (input instanceof URL) {
    urlString = input.toString();
  } else {
    // input is a Request — pull URL + headers + method + body off it.
    urlString = input.url;
    baseInit = {
      method: input.method,
      headers: input.headers,
      // Reading body off a Request consumes it; the test seam passes
      // string/URL so this branch is rarely hit in practice. When it is,
      // we copy what we can and trust the caller passes the body via init.
      ...(init ?? {}),
    };
  }
  try {
    const u = new URL(urlString);
    // Replace the trailing path segment '/embeddings' with '/models/embed'.
    // `base_url_default` ends with `/v1`, so the SDK calls `/v1/embeddings`
    // and we rewrite to `/v1/models/embed`. Use endsWith to avoid mangling
    // any future ZE endpoints that happen to contain 'embeddings' as a
    // substring.
    if (u.pathname.endsWith('/embeddings')) {
      u.pathname = u.pathname.slice(0, -'/embeddings'.length) + '/models/embed';
      urlString = u.toString();
    }
  } catch {
    // Malformed URL — let fetch handle the error.
  }

  // Rewrite request body: inject input_type + encoding_format, strip any
  // base64 the caller smuggled in.
  if (baseInit.body && typeof baseInit.body === 'string') {
    try {
      const parsed = JSON.parse(baseInit.body);
      if (parsed && typeof parsed === 'object') {
        let mutated = false;
        // Force encoding_format: 'float' so the response is a plain
        // float[] and the response-rewriter doesn't need to base64-decode.
        if (parsed.encoding_format !== 'float') {
          parsed.encoding_format = 'float';
          mutated = true;
        }
        // Default input_type when caller didn't thread one (document-side
        // embedding is the correct default for ingest paths).
        if (parsed.input_type === undefined) {
          parsed.input_type = 'document';
          mutated = true;
        }
        if (mutated) {
          const headers = new Headers(baseInit.headers ?? {});
          headers.delete('content-length');
          baseInit = { ...baseInit, body: JSON.stringify(parsed), headers };
        }
      }
    } catch {
      // Body wasn't JSON — pass through untouched.
    }
  }

  const resp = await fetch(urlString, baseInit);
  if (!resp.ok) return resp;
  const ct = resp.headers.get('content-type') ?? '';
  if (!ct.toLowerCase().includes('application/json')) return resp;

  // Layer 1 OOM cap (Content-Length pre-check). Same sizing rationale as
  // Voyage — 256 MB is "unambiguously not a real ZE response" given
  // zembed-1's max 2560-dim × 4 bytes × 16K embeddings = ~160 MB raw.
  const contentLengthHeader = resp.headers.get('content-length');
  if (contentLengthHeader) {
    const len = parseInt(contentLengthHeader, 10);
    if (Number.isFinite(len) && len > MAX_ZEROENTROPY_RESPONSE_BYTES) {
      throw new ZeroEntropyResponseTooLargeError(
        `ZeroEntropy response Content-Length=${len} exceeds ` +
        `${MAX_ZEROENTROPY_RESPONSE_BYTES} bytes — likely compromised endpoint`,
      );
    }
  }

  // INBOUND: rewrite response shape from {results:[{embedding}]} to
  // {data:[{embedding, index}]} so the AI SDK's openai-compatible schema
  // validates. Also map usage.total_tokens → prompt_tokens (SDK requires
  // prompt_tokens when `usage` is present — same divergence Voyage hit at
  // gateway.ts:655).
  try {
    const json: any = await resp.clone().json();
    if (!json || typeof json !== 'object') return resp;
    let modified = false;
    if (Array.isArray(json.results) && !Array.isArray(json.data)) {
      // Layer 2 OOM cap — per-embedding size. ZE returns float[] arrays,
      // so we count the elements × 4 bytes (the float32 width).
      for (const item of json.results) {
        if (item && Array.isArray(item.embedding)) {
          const estBytes = item.embedding.length * 4;
          if (estBytes > MAX_ZEROENTROPY_RESPONSE_BYTES) {
            throw new ZeroEntropyResponseTooLargeError(
              `ZeroEntropy embedding exceeds ${MAX_ZEROENTROPY_RESPONSE_BYTES} ` +
              `bytes (estimated ${estBytes} from ${item.embedding.length} floats)`,
            );
          }
        }
      }
      json.data = json.results.map((r: any, i: number) => ({
        object: 'embedding',
        embedding: r?.embedding ?? [],
        index: i,
      }));
      delete json.results;
      modified = true;
    }
    if (
      json.usage &&
      typeof json.usage === 'object' &&
      json.usage.prompt_tokens === undefined
    ) {
      json.usage.prompt_tokens =
        typeof json.usage.total_tokens === 'number' ? json.usage.total_tokens : 0;
      // SDK also expects total_tokens; ZE provides it directly.
      modified = true;
    }
    if (!modified) return resp;
    return new Response(JSON.stringify(json), {
      status: resp.status,
      statusText: resp.statusText,
      headers: resp.headers,
    });
  } catch (err) {
    // OOM-cap throws MUST propagate. Voyage's pattern: instanceof check on
    // its own tagged class. Same here — only rethrow our own cap class.
    if (err instanceof ZeroEntropyResponseTooLargeError) throw err;
    return resp;
  }
}) as unknown as typeof fetch;

async function resolveEmbeddingProvider(modelStr: string): Promise<{ model: any; recipe: Recipe; modelId: string }> {
  const { parsed, recipe } = resolveRecipe(modelStr);
  assertTouchpoint(recipe, 'embedding', parsed.modelId, getExtendedModelsForProvider(parsed.providerId));
  const cfg = requireConfig();

  const cacheKey = `emb:${recipe.id}:${parsed.modelId}:${cfg.base_urls?.[recipe.id] ?? ''}`;
  const cached = _getModelCache().get(cacheKey);
  if (cached) return { model: cached, recipe, modelId: parsed.modelId };

  const model = instantiateEmbedding(recipe, parsed.modelId, cfg);
  _getModelCache().set(cacheKey, model);
  return { model, recipe, modelId: parsed.modelId };
}

function instantiateEmbedding(recipe: Recipe, modelId: string, cfg: AIGatewayConfig): any {
  switch (recipe.implementation) {
    case 'native-openai': {
      const apiKey = cfg.env.OPENAI_API_KEY;
      if (!apiKey) throw new AIConfigError(
        `OpenAI embedding requires OPENAI_API_KEY.`,
        recipe.setup_hint,
      );
      const client = createOpenAI({ apiKey });
      // AI SDK v6: use .textEmbeddingModel() for embeddings
      return (client as any).textEmbeddingModel
        ? (client as any).textEmbeddingModel(modelId)
        : (client as any).embedding(modelId);
    }
    case 'native-google': {
      const apiKey = cfg.env.GOOGLE_GENERATIVE_AI_API_KEY;
      if (!apiKey) throw new AIConfigError(
        `Google embedding requires GOOGLE_GENERATIVE_AI_API_KEY.`,
        recipe.setup_hint,
      );
      const client = createGoogleGenerativeAI({ apiKey });
      return (client as any).textEmbeddingModel
        ? (client as any).textEmbeddingModel(modelId)
        : (client as any).embedding(modelId);
    }
    case 'native-anthropic':
      throw new AIConfigError(
        `Anthropic has no embedding model. Use openai or google for embeddings.`,
      );
    case 'openai-compatible': {
      // D12=A: unified auth via Recipe.resolveAuth (or default).
      const auth = applyResolveAuth(recipe, cfg, 'embedding');
      // v0.32: env-templated base URL + optional fetch wrapper for Azure.
      const compat = applyOpenAICompatConfig(recipe, cfg);
      // Voyage's openai-compat path needs voyageCompatFetch (translates
      // request/response shape) when the recipe doesn't ship its own fetch
      // wrapper via resolveOpenAICompatConfig. Azure recipes ship their own
      // fetch (api-version splice); voyage doesn't — use voyageCompatFetch.
      // ZeroEntropy needs zeroEntropyCompatFetch (URL path + body input_type
      // + response shape rewrite + OOM caps). Same per-recipe-id branch
      // pattern as voyage so adding a third compat shim is one more case.
      const fetchWrapper =
        compat.fetch ??
        (recipe.id === 'voyage'
          ? voyageCompatFetch
          : recipe.id === 'zeroentropyai'
          ? zeroEntropyCompatFetch
          : undefined);
      const client = createOpenAICompatible({
        name: recipe.id,
        baseURL: compat.baseURL,
        ...(fetchWrapper ? { fetch: fetchWrapper } : {}),
        ...auth,
      });
      return client.textEmbeddingModel(modelId);
    }
    default:
      throw new AIConfigError(`Unknown implementation: ${(recipe as any).implementation}`);
  }
}

/** Minimum sub-batch size before we give up splitting and just throw. */
const MIN_SUB_BATCH = 1;

/**
 * Embed many texts. Truncates to MAX_CHARS, then dispatches based on whether
 * the recipe declares a per-batch token budget.
 *
 * Flow:
 * ```
 * embed(texts)
 *   ├─ resolve recipe + model
 *   ├─ truncate each text to MAX_CHARS (8000)
 *   ├─ read recipe.touchpoints.embedding.{max_batch_tokens, chars_per_token, safety_factor}
 *   │
 *   ├─ if max_batch_tokens declared (Voyage path):
 *   │     budget = max_batch_tokens × shrinkState[recipe].factor (default = recipe.safety_factor)
 *   │     splitByTokenBudget(texts, budget, recipe.chars_per_token)
 *   │     for each sub-batch: embedSubBatch(...)
 *   │
 *   └─ else (OpenAI fast path):
 *         embedSubBatch(texts, ...) once  // no pre-split, no token-limit safety net
 *
 * embedSubBatch(texts, ...)
 *   ├─ try: _embedTransport(texts) → dim check → return Float32Array[]
 *   │       on success: bump shrinkState[recipe].consecutiveSuccesses
 *   │
 *   └─ catch:
 *         if isTokenLimitError(err) AND texts.length > MIN_SUB_BATCH:
 *               shrinkState[recipe].factor *= 0.5     (next embed() pre-splits tighter)
 *               halve at mid=⌈N/2⌉
 *               embedSubBatch(left)  ──┐
 *               embedSubBatch(right) ──┴─ concat in order, return
 *         else:
 *               throw normalizeAIError(err, ...)
 * ```
 *
 * Per-recipe state lives in `_shrinkState` and survives across `embed()`
 * calls within one process. The healing path (after `SHRINK_HEAL_AFTER`
 * consecutive batch successes) walks the factor back toward the recipe's
 * declared `safety_factor` so a transient miss doesn't permanently cap
 * throughput.
 */
/**
 * Per-call passthroughs for `embed()`. Unifies v0.33.4 cancellation/retry
 * controls and v0.35.0.0 asymmetric-input plumbing into one interface so
 * a future passthrough doesn't churn the call signature again.
 *
 * All fields are optional; production callers that don't pass them get
 * unchanged pre-v0.33.4 behavior with document-side encoding (ZE / Voyage
 * v3+ semantics) as the default.
 */
export interface EmbedOpts {
  /**
   * v0.33.4: propagated to Vercel AI SDK's `embedMany({abortSignal})`.
   * When the caller's wall-clock budget fires, an in-flight HTTP request
   * is cancelled within seconds instead of waiting out the provider's
   * HTTP timeout (~30s on OpenAI).
   */
  abortSignal?: AbortSignal;
  /**
   * v0.33.4: propagated to Vercel AI SDK's `embedMany({maxRetries})`.
   * Default in the SDK is 2 (so up to 3 attempts per call). Pass `0` to
   * disable SDK retries when a higher-level wrapper owns the retry
   * policy — otherwise SDK and wrapper retries stack and amplify
   * rate-limit pressure (3 × N wrapper attempts).
   */
  maxRetries?: number;
  /**
   * v0.35.0.0: asymmetric retrieval signal. `'query'` routes through
   * `dimsProviderOptions` so providers that accept query/document
   * encoding (ZE zembed-1, Voyage v3+) produce query-side vectors.
   * Symmetric providers (OpenAI text-3, DashScope, Zhipu) ignore the
   * field. Defaults to undefined (treated as 'document' by the dim
   * resolver — the correct default for indexing paths).
   */
  inputType?: 'query' | 'document';
  /**
   * v0.36 (D10): explicit model override. When set, routes through this
   * provider:model instead of the globally configured embedding_model.
   * Used by the dynamic-embedding-column path so a single query can
   * embed via the provider that matches the active column. NULL/absent
   * preserves the existing global-default behavior.
   *
   * Format: 'provider:model' (e.g. 'voyage:voyage-3-large').
   */
  embeddingModel?: string;
  /**
   * v0.36 (D10): explicit dimensions override, paired with
   * embeddingModel. When set, threads into `dimsProviderOptions` so the
   * gateway sends the right `dimensions` / `output_dimension` to the
   * provider. Must match the dim of the destination column or pgvector
   * rejects the insert/search. NULL preserves the global-default.
   */
  dimensions?: number;
}

export async function embed(texts: string[], opts?: EmbedOpts): Promise<Float32Array[]> {
  if (!texts || texts.length === 0) return [];

  const cfg = requireConfig();
  // v0.36 (D10): caller may override the model. Used by the dynamic-embedding-
  // column path so hybridSearch can embed via the column's provider, not the
  // global default. resolveEmbeddingProvider validates the override at the
  // recipe layer — bad model strings throw AIConfigError with a clear hint.
  const resolveTarget = opts?.embeddingModel ?? getEmbeddingModel();
  const tracker = _getBudgetStore().getStore() ?? null;
  const { model, recipe, modelId } = await resolveEmbeddingProvider(resolveTarget);
  const truncated = texts.map(t => (t ?? '').slice(0, MAX_CHARS));

  // Reserve up front for the worst-case batch token count. Embeddings have
  // no output rate, so maxOutputTokens=0. record() at the end uses the
  // actual total reported by the SDK across all sub-batches.
  if (tracker) {
    const charsPerToken = recipe.touchpoints?.embedding?.chars_per_token ?? DEFAULT_CHARS_PER_TOKEN;
    const totalChars = truncated.reduce((s, t) => s + t.length, 0);
    const estimatedInputTokens = Math.ceil(totalChars / Math.max(charsPerToken, 1));
    tracker.reserve({
      modelId: `${recipe.id}:${modelId}`,
      estimatedInputTokens,
      maxOutputTokens: 0,
      kind: 'embed',
      label: 'gateway.embed',
    });
  }
  // Dim override (D10) — when caller passes `dimensions`, use it. Otherwise
  // fall back to the global cfg default. dimsProviderOptions throws a
  // clear AIConfigError when a Voyage flexible-dim model gets an
  // unsupported value (the existing v0.33.1.1 fail-loud path).
  const effectiveDims = opts?.dimensions ?? cfg.embedding_dimensions ?? DEFAULT_EMBEDDING_DIMENSIONS;
  const providerOpts = dimsProviderOptions(
    recipe.implementation,
    modelId,
    effectiveDims,
    opts?.inputType,
  );
  const expected = effectiveDims;

  const embedding = recipe.touchpoints?.embedding;
  const maxBatchTokens = embedding?.max_batch_tokens;
  const charsPerToken = embedding?.chars_per_token ?? DEFAULT_CHARS_PER_TOKEN;

  // Pre-split is gated on max_batch_tokens. Recipes without it (e.g. OpenAI)
  // ride the fast path: one embedMany call, no recursion safety net.
  const batches = maxBatchTokens
    ? splitByTokenBudget(truncated, Math.floor(maxBatchTokens * effectiveSafetyFactor(recipe)), charsPerToken)
    : [truncated];

  const allEmbeddings: Float32Array[] = [];
  let _embedThrew = false;
  try {
    for (const batch of batches) {
      const result = await embedSubBatch(batch, model, providerOpts, expected, recipe, modelId, opts);
      allEmbeddings.push(...result);
    }
    return allEmbeddings;
  } catch (err) {
    _embedThrew = true;
    throw err;
  } finally {
    if (tracker) {
      // Embed token usage is not surfaced by the AI SDK shape we use; charge
      // based on the truncated input character count using the recipe's
      // chars-per-token. On failure, A3 amended says charge the pessimistic
      // estimate too — embed has no output side, so the input estimate IS
      // the worst case.
      const charsPerToken = recipe.touchpoints?.embedding?.chars_per_token ?? DEFAULT_CHARS_PER_TOKEN;
      const totalChars = truncated.reduce((s, t) => s + t.length, 0);
      const inputTokens = Math.ceil(totalChars / Math.max(charsPerToken, 1));
      try {
        tracker.record({
          modelId: `${recipe.id}:${modelId}`,
          inputTokens,
          outputTokens: 0,
          embeddingDims: expected,
          kind: 'embed',
          label: _embedThrew ? 'gateway.embed.failed' : 'gateway.embed',
        });
      } catch {
        // BudgetExhausted (TX1) — original throw (if any) wins.
      }
    }
  }
}

/**
 * Split texts into sub-batches that stay under the provided budget. Pure;
 * no module state. Exported for the adaptive-embed-batch test suite.
 *
 * @param texts - The texts to partition. Each text counts as
 *   `Math.ceil(text.length / charsPerToken)` tokens for budget purposes.
 * @param budgetTokens - The token ceiling for each sub-batch. Caller is
 *   responsible for applying any safety-factor shrink before passing in.
 * @param charsPerToken - Provider-specific character density. Defaults to
 *   `DEFAULT_CHARS_PER_TOKEN` (4) when omitted, matching OpenAI tiktoken.
 *
 * @internal exported for tests; not part of the public gateway API.
 */
export function splitByTokenBudget(
  texts: string[],
  budgetTokens: number,
  charsPerToken: number = DEFAULT_CHARS_PER_TOKEN,
): string[][] {
  const ratio = charsPerToken > 0 ? charsPerToken : DEFAULT_CHARS_PER_TOKEN;
  const batches: string[][] = [];
  let current: string[] = [];
  let currentTokens = 0;

  for (const text of texts) {
    const estTokens = Math.ceil(text.length / ratio);
    if (current.length > 0 && currentTokens + estTokens > budgetTokens) {
      batches.push(current);
      current = [];
      currentTokens = 0;
    }
    current.push(text);
    currentTokens += estTokens;
  }
  if (current.length > 0) batches.push(current);

  return batches;
}

/**
 * Returns true if the error looks like a provider batch-token-limit error.
 *
 * @internal exported for tests; not part of the public gateway API.
 */
export function isTokenLimitError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return (
    /max.*allowed.*tokens.*batch/i.test(msg) ||
    /batch.*too.*many.*tokens/i.test(msg) ||
    /token.*limit.*exceeded/i.test(msg)
  );
}

/**
 * Resolve the recipe's effective safety factor (declared default, optionally
 * shrunk by prior misses in this process).
 */
function effectiveSafetyFactor(recipe: Recipe): number {
  const declared = recipe.touchpoints?.embedding?.safety_factor ?? DEFAULT_SAFETY_FACTOR;
  const entry = _getShrinkState().get(recipe.id);
  return entry?.factor ?? declared;
}

/** Tighten the recipe's effective safety factor on a token-limit miss. */
function shrinkOnMiss(recipe: Recipe): void {
  const declared = recipe.touchpoints?.embedding?.safety_factor ?? DEFAULT_SAFETY_FACTOR;
  const current = _getShrinkState().get(recipe.id)?.factor ?? declared;
  const next = Math.max(SHRINK_FLOOR, current * 0.5);
  _getShrinkState().set(recipe.id, { factor: next, consecutiveSuccesses: 0 });
}

/** Bump the win counter; heal toward declared default after enough wins. */
function recordSubBatchSuccess(recipe: Recipe): void {
  const declared = recipe.touchpoints?.embedding?.safety_factor ?? DEFAULT_SAFETY_FACTOR;
  const entry = _getShrinkState().get(recipe.id);
  if (!entry || entry.factor >= declared) {
    // Either no shrink active, or already at/above the declared ceiling — nothing to heal.
    if (entry) {
      _getShrinkState().set(recipe.id, { factor: entry.factor, consecutiveSuccesses: 0 });
    }
    return;
  }
  const wins = entry.consecutiveSuccesses + 1;
  if (wins >= SHRINK_HEAL_AFTER) {
    const healed = Math.min(declared, entry.factor * 1.5);
    _getShrinkState().set(recipe.id, { factor: healed, consecutiveSuccesses: 0 });
  } else {
    _getShrinkState().set(recipe.id, { factor: entry.factor, consecutiveSuccesses: wins });
  }
}

/**
 * Read the current shrink state for a recipe. Test-only seam.
 *
 * @internal exported for tests; not part of the public gateway API.
 */
export function __getShrinkStateForTests(recipeId: string): ShrinkEntry | undefined {
  const entry = _getShrinkState().get(recipeId);
  return entry ? { ...entry } : undefined;
}

/**
 * Embed a single sub-batch with automatic halving on token-limit errors.
 * If the batch is already at MIN_SUB_BATCH and still fails, throws.
 */
async function embedSubBatch(
  texts: string[],
  model: any,
  providerOpts: any,
  expectedDims: number,
  recipe: Recipe,
  modelId: string,
  opts?: EmbedOpts,
): Promise<Float32Array[]> {
  try {
    const result = await _getEmbedTransport()({
      model,
      values: texts,
      providerOptions: providerOpts,
      // v0.33.4: caller-supplied abortSignal + maxRetries passthrough.
      // Undefined fields are ignored by the AI SDK so the call shape stays
      // identical for production callers that don't opt in.
      ...(opts?.abortSignal !== undefined && { abortSignal: opts.abortSignal }),
      ...(opts?.maxRetries !== undefined && { maxRetries: opts.maxRetries }),
    });

    if (!Array.isArray(result.embeddings) || result.embeddings.length !== texts.length) {
      throw new AIConfigError(
        `Embedding provider returned ${result.embeddings?.length ?? 0} embedding(s) for ${texts.length} input(s).`,
        `Retry the import after checking provider health; partial embedding responses are not safe to index.`,
      );
    }

    for (const embedding of result.embeddings) {
      if (Array.isArray(embedding) && embedding.length !== expectedDims) {
        throw new AIConfigError(
          `Embedding dim mismatch: model ${modelId} returned ${embedding.length} but schema expects ${expectedDims}.`,
          `Run \`gbrain migrate --embedding-model ${getEmbeddingModel()} --embedding-dimensions ${embedding.length}\` or change models.`,
        );
      }
    }

    recordSubBatchSuccess(recipe);
    return result.embeddings.map((e: number[]) => new Float32Array(e));
  } catch (err) {
    // On token-limit error, tighten the recipe's effective safety factor
    // (so the next embed() pre-splits smaller) and recursively halve THIS
    // batch to make forward progress without dropping work.
    if (isTokenLimitError(err) && texts.length > MIN_SUB_BATCH) {
      shrinkOnMiss(recipe);
      const mid = Math.ceil(texts.length / 2);
      const left = await embedSubBatch(texts.slice(0, mid), model, providerOpts, expectedDims, recipe, modelId, opts);
      const right = await embedSubBatch(texts.slice(mid), model, providerOpts, expectedDims, recipe, modelId, opts);
      return [...left, ...right];
    }
    throw normalizeAIError(err, `embed(${recipe.id}:${modelId})`);
  }
}

/** Embed one text (convenience wrapper). */
export async function embedOne(text: string): Promise<Float32Array> {
  const [v] = await embed([text]);
  return v;
}

/**
 * v0.35.0.0+: embed a single text on the QUERY side of an asymmetric retrieval
 * pipeline. Threads `inputType: 'query'` into `dimsProviderOptions`, which
 * for ZE (`zembed-1`) and Voyage v3+ models emits `input_type: 'query'` into
 * the request body so the provider returns query-side vectors. For
 * symmetric providers (OpenAI text-3, DashScope, Zhipu) the field is dropped
 * — no behavior change.
 *
 * Two call sites in v0.33.2: vector seed embed at hybrid.ts:400 (cache miss
 * path) and cache lookup embed at hybrid.ts:629. All ingest paths (sync,
 * import, embed CLI) continue to use `embed()` which defaults to document
 * encoding.
 *
 * Returns a single Float32Array (not a batch).
 */
export async function embedQuery(
  text: string,
  opts?: { embeddingModel?: string; dimensions?: number },
): Promise<Float32Array> {
  const [v] = await embed([text], {
    inputType: 'query',
    embeddingModel: opts?.embeddingModel,
    dimensions: opts?.dimensions,
  });
  return v;
}

// ---- Multimodal embedding (v0.27.1) ----

/** Voyage multimodal API caps at 32 inputs per request. */
const MULTIMODAL_BATCH_SIZE = 32;
/** Voyage caps each image at 20MB; the caller enforces, this is documentation. */
const MULTIMODAL_MAX_IMAGE_BYTES = 20 * 1024 * 1024;

/**
 * v0.27.1: embed multimodal inputs (images today; video keyframes once
 * Voyage 3.5 multimodal ships). Routes to the recipe's multimodal endpoint
 * via direct fetch — Vercel AI SDK has no multimodal-embedding abstraction
 * yet so we bypass it. Reuses the existing API-key resolution and
 * dim-mismatch error pattern from embed().
 *
 * Today: Voyage-only. Other recipes throw AIConfigError pointing at the
 * v0.28+ TODOs that add OpenAI/Cohere multimodal.
 *
 * Returns one Float32Array per input, in input order.
 *
 * Empty input → returns []. Preserves the `embed([])` contract.
 */
export async function embedMultimodal(
  inputs: MultimodalInput[],
  opts: EmbedMultimodalOpts = {},
): Promise<Float32Array[]> {
  if (!inputs || inputs.length === 0) return [];

  const cfg = requireConfig();
  // Prefer embedding_multimodal_model when set, so brains using OpenAI for
  // text embeddings can route multimodal to Voyage without changing the
  // primary embedding_model. Falls back to embedding_model for single-model setups.
  const modelStr = cfg.embedding_multimodal_model
    ?? cfg.embedding_model
    ?? DEFAULT_EMBEDDING_MODEL;
  const { parsed, recipe } = resolveRecipe(modelStr);
  const touchpoint = recipe.touchpoints.embedding;
  if (!touchpoint?.supports_multimodal) {
    throw new AIConfigError(
      `Recipe ${recipe.id} (${parsed.modelId}) does not support multimodal embedding.`,
      `Set embedding_multimodal_model to route multimodal separately from text embeddings.\n` +
      `Today: voyage:voyage-multimodal-3. OpenAI / Cohere multimodal support is on the roadmap.`,
    );
  }
  // v0.28.11: model-level validation. supports_multimodal is recipe-scoped, so
  // a recipe like Voyage that mixes text-only models with one multimodal model
  // would otherwise let `voyage:voyage-3-large` through and fail at the
  // /multimodalembeddings endpoint. When the recipe declares an explicit
  // multimodal_models allow-list, enforce it pre-flight.
  if (touchpoint.multimodal_models && !touchpoint.multimodal_models.includes(parsed.modelId)) {
    throw new AIConfigError(
      `${recipe.id}:${parsed.modelId} is not a multimodal-capable model.`,
      `Use one of: ${touchpoint.multimodal_models.map(m => `${recipe.id}:${m}`).join(', ')}.`,
    );
  }

  // v0.34.1 (#875): route by recipe.implementation so openai-compat
  // providers (LiteLLM, Anyscale, vLLM, etc.) reach the standard
  // /embeddings endpoint with multimodal content arrays. The Voyage
  // recipe is `openai-compat` per tier but uses its own /multimodalembeddings
  // path, so we still branch on recipe.id for that one.
  if (recipe.id !== 'voyage' && recipe.implementation === 'openai-compatible') {
    return embedMultimodalOpenAICompat(inputs, recipe, parsed.modelId, cfg, opts);
  }
  if (recipe.id !== 'voyage') {
    throw new AIConfigError(
      `Multimodal embedding for recipe ${recipe.id} (${recipe.implementation}) is not implemented yet. ` +
      `Today: voyage (own endpoint), openai-compatible recipes (standard /embeddings with content arrays).`,
    );
  }

  const apiKey = cfg.env[recipe.auth_env?.required[0] ?? 'VOYAGE_API_KEY'];
  if (!apiKey) {
    throw new AIConfigError(
      `${recipe.name} requires ${recipe.auth_env?.required[0]} for multimodal embedding.`,
      recipe.setup_hint,
    );
  }
  const baseUrl = cfg.base_urls?.[recipe.id] ?? recipe.base_url_default;
  if (!baseUrl) {
    throw new AIConfigError(
      `${recipe.name} requires a base URL for multimodal embedding.`,
      recipe.setup_hint,
    );
  }

  const expected = cfg.embedding_dimensions ?? DEFAULT_EMBEDDING_DIMENSIONS;
  // Voyage multimodal returns 1024 dims. If the brain is configured for a
  // different `embedding` column dim (e.g. OpenAI 1536 text), the dual-column
  // schema lets text live in `embedding` (1536) and images in
  // `embedding_image` (1024). The gateway-level dim assertion only fires when
  // the caller is targeting the primary `embedding` column; for image rows
  // landing in `embedding_image` the column itself is fixed at 1024.
  const targetDims = 1024;

  // v0.36 (D22-2): thread Voyage's retrieval input_type discipline through.
  // Default 'document' preserves pre-v0.36 ingest behavior.
  const inputType = opts.inputType ?? 'document';

  // Batch in groups of 32 (Voyage's published max). Each batch is one HTTP
  // call; results concatenate in input order.
  const allEmbeddings: Float32Array[] = [];
  for (let i = 0; i < inputs.length; i += MULTIMODAL_BATCH_SIZE) {
    const batch = inputs.slice(i, i + MULTIMODAL_BATCH_SIZE);
    const body = {
      inputs: batch.map(input => ({
        // Voyage's documented content shape supports both image and text
        // entries. v0.36 cross-modal: text variant for query embedding.
        content: [
          input.kind === 'text'
            ? { type: 'text', text: input.text }
            : {
              type: 'image_base64',
              image_base64: `data:${input.mime};base64,${input.data}`,
            },
        ],
      })),
      model: parsed.modelId,
      input_type: inputType,
    };

    let res: Response;
    try {
      res = await fetch(`${baseUrl}/multimodalembeddings`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify(body),
      });
    } catch (err) {
      throw normalizeAIError(err, `embedMultimodal(${recipe.id}:${parsed.modelId})`);
    }

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      if (res.status === 401 || res.status === 403) {
        throw new AIConfigError(
          `Voyage multimodal returned ${res.status}: ${text || 'auth failed'}.`,
          `Re-export ${recipe.auth_env?.required[0]} or rotate the key at ${recipe.auth_env?.setup_url}.`,
        );
      }
      // 429 / 5xx are transient; let the caller retry.
      throw new AITransientError(
        `Voyage multimodal returned ${res.status}: ${text || 'transient error'}.`,
      );
    }

    let parsedBody: { data?: Array<{ embedding: number[] }> };
    try {
      parsedBody = (await res.json()) as { data?: Array<{ embedding: number[] }> };
    } catch (err) {
      throw new AITransientError(
        `Voyage multimodal returned malformed JSON: ${err instanceof Error ? err.message : String(err)}.`,
      );
    }
    if (!parsedBody.data || !Array.isArray(parsedBody.data) || parsedBody.data.length !== batch.length) {
      throw new AITransientError(
        `Voyage multimodal returned unexpected payload shape (expected ${batch.length} embeddings).`,
      );
    }

    for (const row of parsedBody.data) {
      if (!Array.isArray(row.embedding) || row.embedding.length !== targetDims) {
        throw new AIConfigError(
          `Voyage multimodal returned ${row.embedding?.length ?? 0}-dim vector; expected ${targetDims}.`,
          `Voyage multimodal-3 is fixed at 1024 dims. Brain primary embedding dim is ${expected} ` +
          `(used by the text path). Image vectors land in content_chunks.embedding_image (1024).`,
        );
      }
      allEmbeddings.push(new Float32Array(row.embedding));
    }
  }

  return allEmbeddings;
}

// Documentation pointer: callers must size-check before calling. Voyage caps
// each input at MULTIMODAL_MAX_IMAGE_BYTES (20MB). importImageFile enforces
// this and routes oversize files to sync_failures.jsonl.
void MULTIMODAL_MAX_IMAGE_BYTES;

/**
 * v0.34.1 (#875): multimodal embedding via the standard OpenAI-compatible
 * `/embeddings` endpoint. Many providers fronted by LiteLLM (Anyscale, vLLM,
 * native OpenAI fed multimodal models) accept content arrays where each
 * element is either `{type: "input_text", text: "..."}` or
 * `{type: "image_url", image_url: {url: "data:..."}}` and return the same
 * `{data: [{embedding: number[]}, ...]}` shape as text embeddings.
 *
 * Routing comes from gateway.embedMultimodal when the recipe's implementation
 * is 'openai-compatible' and recipe.id is not 'voyage' (which has its own
 * /multimodalembeddings path).
 *
 * D12 dim validation: the response is checked against the recipe's
 * declared `default_dims` or the brain's `embedding_dimensions` config.
 * Mismatch throws AIConfigError with a paste-ready hint pointing at the
 * model picker — preferable to a silent corrupt-storage failure when the
 * brain's vector(N) column rejects the row.
 */
async function embedMultimodalOpenAICompat(
  inputs: MultimodalInput[],
  recipe: Recipe,
  modelId: string,
  cfg: AIGatewayConfig,
  opts: EmbedMultimodalOpts = {},
): Promise<Float32Array[]> {
  // Auth resolution via the gateway's canonical helper so LiteLLM-style
  // optional-auth recipes (Authorization: Bearer LITELLM_API_KEY) and
  // hard-required-auth recipes (OpenAI Authorization: Bearer
  // OPENAI_API_KEY) both work via the same code path. Throws AIConfigError
  // when required env is missing.
  const authResult = recipe.resolveAuth
    ? recipe.resolveAuth(cfg.env)
    : defaultResolveAuth(recipe, cfg.env, 'embedding');
  const baseUrl = cfg.base_urls?.[recipe.id] ?? recipe.base_url_default;
  if (!baseUrl) {
    throw new AIConfigError(
      `${recipe.name} requires a base URL for multimodal embedding.`,
      recipe.setup_hint,
    );
  }

  // D12 — dim validation. Prefer recipe's declared default_dims when set;
  // fall back to the brain's configured embedding_dimensions. If neither
  // is known (LiteLLM recipe with default_dims=0 and no config override),
  // we skip the dim check rather than fabricate an expected value — the
  // engine's vector(N) column will reject mismatched rows at INSERT time
  // with a clearer error than anything we could throw here.
  const recipeDims = recipe.touchpoints.embedding?.default_dims ?? 0;
  const expectedDims = recipeDims > 0
    ? recipeDims
    : (cfg.embedding_dimensions ?? 0);

  // Send each input as one /embeddings request. Most providers cap the
  // number of inputs per call at the text-embedding batch limit, but the
  // multimodal content array varies per provider. Single-input requests
  // are the safe lowest common denominator; LiteLLM's proxy backend
  // batches internally if it can.
  // v0.36 (D22-2): inputType opt threaded for symmetry with the Voyage path.
  // Most openai-compatible proxies don't forward this field, but recording
  // it in the body keeps LiteLLM-style providers that DO accept it correct.
  const inputType = opts.inputType ?? 'document';

  const allEmbeddings: Float32Array[] = [];
  for (const input of inputs) {
    const body: Record<string, unknown> = {
      model: modelId,
      input: [
        input.kind === 'text'
          ? { type: 'input_text', text: input.text }
          : {
            // OpenAI's documented multimodal content shape. The data-URL
            // form embeds the image bytes inline so the proxy doesn't need
            // network access to fetch the image.
            type: 'image_url',
            image_url: { url: `data:${input.mime};base64,${input.data}` },
          },
      ],
      input_type: inputType,
    };

    let res: Response;
    try {
      res = await fetch(`${baseUrl}/embeddings`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          [authResult.headerName]: authResult.token,
        },
        body: JSON.stringify(body),
      });
    } catch (err) {
      throw normalizeAIError(err, `embedMultimodal(${recipe.id}:${modelId})`);
    }

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      if (res.status === 401 || res.status === 403) {
        const requiredKey = recipe.auth_env?.required[0];
        throw new AIConfigError(
          `${recipe.name} multimodal returned ${res.status}: ${text || 'auth failed'}.`,
          requiredKey
            ? `Re-export ${requiredKey} or rotate the key at ${recipe.auth_env?.setup_url ?? recipe.setup_hint}.`
            : recipe.setup_hint,
        );
      }
      // Surface the upstream error verbatim — 400s here usually mean the
      // proxied model doesn't support multimodal input. The error text is
      // the user's best signal for picking a different model id.
      throw new AITransientError(
        `${recipe.name} multimodal returned ${res.status}: ${text || 'transient error'}.`,
      );
    }

    let parsedBody: { data?: Array<{ embedding: number[] }> };
    try {
      parsedBody = (await res.json()) as { data?: Array<{ embedding: number[] }> };
    } catch (err) {
      throw new AITransientError(
        `${recipe.name} multimodal returned malformed JSON: ${err instanceof Error ? err.message : String(err)}.`,
      );
    }
    if (!parsedBody.data || !Array.isArray(parsedBody.data) || parsedBody.data.length < 1) {
      throw new AITransientError(
        `${recipe.name} multimodal returned no embeddings (expected 1).`,
      );
    }

    const row = parsedBody.data[0];
    if (!Array.isArray(row.embedding)) {
      throw new AITransientError(
        `${recipe.name} multimodal returned non-array embedding payload.`,
      );
    }
    // D12 — dim validation. Throw EmbedDimensionMismatchError-shape error
    // (AIConfigError with model id + observed + expected so the operator
    // can diagnose and pick a compatible model OR adjust the brain's
    // embedding_dimensions config). Skip the check when expectedDims=0
    // (no recipe declaration AND no config override).
    if (expectedDims > 0 && row.embedding.length !== expectedDims) {
      throw new AIConfigError(
        `${recipe.id}:${modelId} returned ${row.embedding.length}-dim vector; expected ${expectedDims}.`,
        `The brain's embedding column is fixed at ${expectedDims} dims; this model is incompatible. ` +
        `Either pick a model that returns ${expectedDims} dims, OR set --embedding-dimensions ${row.embedding.length} ` +
        `and reinitialize the embedding column at the new width.`,
      );
    }
    allEmbeddings.push(new Float32Array(row.embedding));
  }

  return allEmbeddings;
}

// ---- v0.36 cross-modal wave: query-side multimodal embedding + safe variant ----

/**
 * Embed a TEXT query through the configured multimodal model.
 *
 * Routes through `embedding_multimodal_model` (defaults to Voyage multimodal-3)
 * so the resulting vector lives in the multimodal embedding space — the same
 * space the brain's `embedding_image` column was populated into. A text
 * query embedded here can match image chunks (Phase 1 of the cross-modal
 * wave) and, post Phase 3 reindex, text chunks in the unified column.
 *
 * Threads `inputType: 'query'` (D22-2) so Voyage routes to the retrieval
 * half of its asymmetric embedding space.
 *
 * Sibling of v0.35.0.0's `embedQuery(text)`, which uses the TEXT embedding
 * model (typically OpenAI text-embedding-3-large at 1536d or 2560d, NOT
 * compatible with the 1024d multimodal column).
 */
export async function embedQueryMultimodal(text: string): Promise<Float32Array> {
  const [vec] = await embedMultimodal([{ kind: 'text', text }], { inputType: 'query' });
  if (!vec) {
    throw new AITransientError('embedQueryMultimodal: gateway returned no vector for non-empty text input');
  }
  return vec;
}

/**
 * Embed an IMAGE as a query through the configured multimodal model.
 *
 * Sibling of `embedQueryMultimodal(text)` for the Phase 2 image-as-query
 * path. The image bytes must already be loaded and base64-encoded by the
 * caller (see `src/core/search/image-loader.ts` for the SSRF-defended
 * loader). Threads `inputType: 'query'` so Voyage routes to the
 * retrieval half of its asymmetric space.
 */
export async function embedQueryMultimodalImage(
  input: { data: string; mime: string },
): Promise<Float32Array> {
  const [vec] = await embedMultimodal(
    [{ kind: 'image_base64', data: input.data, mime: input.mime }],
    { inputType: 'query' },
  );
  if (!vec) {
    throw new AITransientError('embedQueryMultimodalImage: gateway returned no vector');
  }
  return vec;
}

/**
 * Partial-failure-aware variant of `embedMultimodal`.
 *
 * The default `embedMultimodal()` throws on first failure to preserve the
 * pre-v0.36 contract (used by `importImageFile` which can't proceed on
 * partial data). Phase 3 `reindex --multimodal` ingests many thousands
 * of chunks and CAN make forward progress with partial results — it
 * uses this variant so a 401 on chunk 87K doesn't discard the 31
 * already-computed embeddings in that batch.
 *
 * Strategy:
 *   1. Try the full input set via `embedMultimodal`. On success, return.
 *   2. On AIConfigError (permanent), surface every input as failed —
 *      the misconfig isn't going to fix itself by retrying smaller.
 *   3. On AITransientError or other thrown error, split-and-retry
 *      via binary search. Single-input attempts that fail are recorded
 *      in `failedIndices` and skipped.
 *
 * Returns `MultimodalBatchResult` with parallel-indexed `embeddings`
 * (undefined for failed slots) and a `failedIndices` array.
 */
export async function embedMultimodalSafe(
  inputs: MultimodalInput[],
  opts: EmbedMultimodalOpts = {},
): Promise<MultimodalBatchResult> {
  if (!inputs || inputs.length === 0) {
    return { embeddings: [], failedIndices: [] };
  }

  const embeddings: Array<Float32Array | undefined> = new Array(inputs.length).fill(undefined);
  const failedIndices: number[] = [];
  let lastError: Error | undefined;

  async function attempt(startIdx: number, items: MultimodalInput[]): Promise<void> {
    if (items.length === 0) return;
    try {
      const vecs = await embedMultimodal(items, opts);
      for (let i = 0; i < vecs.length; i++) {
        embeddings[startIdx + i] = vecs[i];
      }
      return;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      // AIConfigError = permanent misconfig. Retrying smaller won't help.
      if (lastError instanceof AIConfigError) {
        for (let i = 0; i < items.length; i++) failedIndices.push(startIdx + i);
        return;
      }
      // Single input that failed — record and move on.
      if (items.length === 1) {
        failedIndices.push(startIdx);
        return;
      }
      // Binary-search split. Each half gets its own retry.
      const mid = Math.floor(items.length / 2);
      await attempt(startIdx, items.slice(0, mid));
      await attempt(startIdx + mid, items.slice(mid));
    }
  }

  await attempt(0, inputs);
  failedIndices.sort((a, b) => a - b);

  return { embeddings, failedIndices, lastError };
}
