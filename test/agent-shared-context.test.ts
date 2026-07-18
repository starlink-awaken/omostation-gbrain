/**
 * G-DEL.4 — drive real AgentSharedContextStore + KOS adapter (no reimplementation).
 */
import { describe, test, expect } from "bun:test";
import {
  AgentSharedContextStore,
  createKosRetrieve,
  formatSharedContextForKos,
  seedSharedContextIntoKos,
  type KosDocumentRow,
} from "../src/core/agent-shared-context.ts";

describe("AgentSharedContextStore cross-agent R/W", () => {
  test("agent-A write is visible to agent-B when shared (empty readers = all)", () => {
    const store = new AgentSharedContextStore("task-1");
    const written = store.write("agent-A", "plan.summary", "ship registry first", {
      scope: "task-1",
      tags: ["g-del.4", "bet-b7da"],
    });
    expect(written.writer).toBe("agent-A");
    expect(written.key).toBe("plan.summary");

    const readByB = store.read("agent-B", "plan.summary", { scope: "task-1" });
    expect(readByB).not.toBeNull();
    expect(readByB!.value).toBe("ship registry first");
    expect(readByB!.writer).toBe("agent-A");
  });

  test("private readers list hides key from non-listed agent", () => {
    const store = new AgentSharedContextStore();
    store.write("agent-A", "secret", "classified", {
      readers: ["agent-A", "agent-C"],
    });
    expect(store.read("agent-B", "secret")).toBeNull();
    expect(store.read("agent-C", "secret")?.value).toBe("classified");
  });

  test("listVisible only returns authorized records", () => {
    const store = new AgentSharedContextStore();
    store.write("agent-A", "pub", "public");
    store.write("agent-A", "priv", "private", { readers: ["agent-A"] });
    const visible = store.listVisible("agent-B");
    expect(visible.map((r) => r.key)).toEqual(["pub"]);
  });
});

describe("KOS retrieve adapter", () => {
  test("seedSharedContextIntoKos + createKosRetrieve returns non-empty for seeded query", () => {
    const store = new AgentSharedContextStore();
    store.write("agent-A", "collab.handoff", "G-DEL.2a contract ready for implementers", {
      scope: "bet-b7da",
      tags: ["shared-memory"],
    });
    const records = store.exportScope("bet-b7da");
    expect(records.length).toBe(1);

    // In-memory KOS documents table
    const docs: KosDocumentRow[] = [];
    const n = seedSharedContextIntoKos(records, "bet-b7da", (row) => {
      docs.push(row);
    });
    expect(n).toBe(1);
    expect(docs[0].canonical_path).toContain("shared-context/bet-b7da/collab.handoff");
    expect(docs[0].body_preview).toContain("G-DEL.2a contract ready");

    const retrieve = createKosRetrieve((sql, params) => {
      // Minimal LIKE simulator for the injected query shape
      expect(sql).toContain("FROM documents");
      const like = String(params[0] ?? "").replace(/%/g, "");
      return docs.filter(
        (d) =>
          d.title.includes(like) ||
          d.canonical_path.includes(like) ||
          d.body_preview.includes(like),
      );
    });

    const hits = retrieve("collab.handoff");
    expect(hits.length).toBeGreaterThan(0);
    expect(hits[0].path).toContain("collab.handoff");
    expect(hits[0].preview.length).toBeGreaterThan(0);
  });

  test("formatSharedContextForKos embeds writer identity", () => {
    const body = formatSharedContextForKos(
      {
        key: "k",
        value: "v",
        writer: "agent-X",
        writtenAt: "2026-07-18T00:00:00.000Z",
        readers: [],
      },
      "s",
    );
    expect(body).toContain("writer: agent-X");
    expect(body).toContain("v");
  });
});
