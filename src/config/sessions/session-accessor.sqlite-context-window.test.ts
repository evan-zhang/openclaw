import path from "node:path";
import { expect, it } from "vitest";
import { CURRENT_SESSION_VERSION, SessionManager } from "../../agents/sessions/session-manager.js";
import { requireNodeSqlite } from "../../infra/node-sqlite.js";
import { openOpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import {
  appendTranscriptEvent,
  appendTranscriptMessage,
  upsertSessionEntryCore,
} from "./session-accessor.js";
import { readSessionTranscriptBoundedActiveContextCore } from "./session-accessor.sqlite-active-context.js";
import { readActiveTranscriptStats as readSessionTranscriptActiveStats } from "./session-accessor.sqlite-history.test-support.js";
import { seedUnindexedTranscriptForTest } from "./session-accessor.sqlite-import.test-support.js";
import { waitForSessionTranscriptProjection } from "./session-transcript-reconcile.js";

async function withBoundedContextScope(
  run: (scope: {
    agentId: string;
    sessionId: string;
    sessionKey: string;
    storePath: string;
  }) => Promise<void>,
): Promise<void> {
  await withOpenClawTestState({ label: "bounded-transcript-context" }, async (state) => {
    const scope = {
      agentId: "main",
      sessionId: "bounded-context",
      sessionKey: "agent:main:bounded-context",
      storePath: path.join(state.sessionsDir("main"), "sessions.json"),
    };
    await upsertSessionEntryCore(scope, { sessionId: scope.sessionId, updatedAt: 1 });
    await run(scope);
  });
}

async function seedImportedTranscript(
  scope: Parameters<Parameters<typeof withBoundedContextScope>[0]>[0],
  events: unknown[],
) {
  await seedUnindexedTranscriptForTest({
    ...scope,
    entry: { sessionId: scope.sessionId, updatedAt: 1 },
    events: events.map((event, seq) => ({
      session_id: scope.sessionId,
      seq,
      created_at: seq,
      event_json: JSON.stringify(event),
    })),
  });
}

it.each(["indexed", "unindexed"] as const)(
  "bounds the latest semantic window instead of %s compacted ancestors",
  async (indexing) => {
    await withBoundedContextScope(async (scope) => {
      const events = [
        { type: "session", version: CURRENT_SESSION_VERSION, id: scope.sessionId },
        {
          type: "message",
          id: "old",
          parentId: null,
          message: { role: "user", content: "synthetic-old-history:" + "x".repeat(32_000) },
        },
        {
          type: "message",
          id: "kept",
          parentId: "old",
          message: {
            role: "user",
            content: "Pending task: retain the output path and finish validation.",
          },
        },
        {
          type: "compaction",
          id: "cut",
          parentId: "kept",
          firstKeptEntryId: "kept",
          summary: "Task state: validation pending; output /synthetic/result.png",
          tokensBefore: 100,
        },
        {
          type: "message",
          id: "tail",
          parentId: "cut",
          message: { role: "assistant", content: "Continue validation." },
        },
      ];
      if (indexing === "unindexed") {
        await seedImportedTranscript(scope, events);
      } else {
        for (const event of events) {
          if (event.type === "message") {
            await appendTranscriptMessage(scope, {
              eventId: event.id,
              parentId: event.parentId,
              message: event.message!,
            });
          } else {
            await appendTranscriptEvent(scope, event);
          }
        }
        await waitForSessionTranscriptProjection(scope);
      }
      const stats = readSessionTranscriptActiveStats(scope);
      expect(stats.sizeBytes).toBeLessThan(4096);
      const context = readSessionTranscriptBoundedActiveContextCore(scope, {
        maxBytes: 4096,
        maxEvents: 10,
      });
      expect(context.truncated).toBe(false);
      expect(context.events.map((event) => (event as { id: string }).id)).toEqual([
        scope.sessionId,
        "kept",
        "cut",
        "tail",
      ]);
      expect(context.firstKeptRanges.get("cut")).toEqual({ startIndex: 1, endIndex: 2 });
      expect(context.serializedBytes).toBe(
        stats.sizeBytes + Buffer.byteLength(JSON.stringify(context.events[0])) + 1,
      );
      await appendTranscriptMessage(scope, {
        eventId: "oversized-current",
        parentId: "tail",
        message: { role: "user", content: "current".repeat(2000), timestamp: 3 },
      });
      expect(
        readSessionTranscriptBoundedActiveContextCore(scope, {
          maxBytes: 4096,
          maxEvents: 10,
        }).truncated,
      ).toBe(true);
      await appendTranscriptEvent(scope, {
        type: "compaction",
        id: "second-cut",
        parentId: "oversized-current",
        firstKeptEntryId: "second-cut",
        summary: "Still pending: /synthetic/result.png",
        tokensBefore: 100,
      });
      const second = readSessionTranscriptBoundedActiveContextCore(scope, {
        maxBytes: 4096,
        maxEvents: 10,
      });
      expect(second.truncated).toBe(false);
      expect(second.boundaryCount).toBe(2);
      expect(second.events.map((event) => (event as { id: string }).id)).toEqual([
        scope.sessionId,
        "tail",
        "second-cut",
      ]);
    });
  },
);

it.each(["indexed", "unindexed"] as const)(
  "preserves retained projections and the latest %s checkpoint chain without old full snapshots",
  async (indexing) => {
    await withBoundedContextScope(async (scope) => {
      const snapshot = {
        prunedToolResults: [],
        ambiguousToolResultBaseKeys: [],
        frozenToolResults: [{ key: "result", sourceHash: "synthetic", texts: ["frozen text"] }],
      };
      const full = { marker: snapshot };
      const delta = {
        marker: {
          cacheTtlDelta: {
            version: 1,
            seq: 1,
            baseHash: "synthetic-base",
            resultHash: "synthetic-result",
          },
        },
      };
      const entries = [
        { type: "session", version: CURRENT_SESSION_VERSION, id: scope.sessionId },
        {
          type: "custom",
          id: "old-cache",
          parentId: null,
          customType: "openclaw.cache-ttl",
          data: {
            ...snapshot,
            frozenToolResults: [{ key: "old", sourceHash: "old", texts: ["x".repeat(128_000)] }],
          },
        },
        {
          type: "thinking_level_change",
          id: "thinking",
          parentId: "old-cache",
          thinkingLevel: "high",
        },
        {
          type: "model_change",
          id: "model",
          parentId: "thinking",
          provider: "synthetic",
          modelId: "synthetic-model",
        },
        {
          type: "custom_message",
          id: "kept-projection",
          parentId: "model",
          customType: "task",
          content: "pending validation",
          display: true,
        },
        {
          type: "custom",
          id: "obsolete-kept-cache",
          parentId: "kept-projection",
          customType: "openclaw.cache-ttl",
          data: {
            ...snapshot,
            frozenToolResults: [
              { key: "old-kept", sourceHash: "synthetic", texts: ["x".repeat(128_000)] },
            ],
          },
        },
        {
          type: "custom",
          id: "full",
          parentId: "obsolete-kept-cache",
          customType: "openclaw.cache-ttl",
          data: full.marker,
        },
        {
          type: "custom",
          id: "delta",
          parentId: "full",
          customType: "openclaw.cache-ttl",
          data: delta.marker,
        },
        {
          type: "branch_summary",
          fromId: "synthetic-branch-origin",
          id: "kept-branch",
          parentId: "delta",
          summary: "Keep branch state",
        },
        {
          type: "compaction",
          id: "cut",
          parentId: "kept-branch",
          firstKeptEntryId: "kept-projection",
          summary: "Task pending",
          tokensBefore: 100,
        },
      ];
      const timestamped = entries.map((entry) =>
        Object.assign(entry, {
          timestamp: "2026-10-12T00:00:00.000Z",
        }),
      );
      if (indexing === "unindexed") {
        await seedImportedTranscript(scope, timestamped);
        // Imported identity rows can predate compressed payloads and navigation envelopes.
        const database = openOpenClawAgentDatabase(scope);
        const { DatabaseSync } = requireNodeSqlite();
        const writer = new DatabaseSync(database.path);
        try {
          const legacy = writer.prepare(
            "UPDATE transcript_events SET event_json = ?, event_zstd = NULL, navigation_json = NULL WHERE session_id = ? AND seq = ?",
          );
          for (const [seq, entry] of timestamped.entries()) {
            if (entry.id === "old-cache" || entry.id === "obsolete-kept-cache") {
              legacy.run(JSON.stringify(entry), scope.sessionId, seq);
            }
          }
          expect(
            writer
              .prepare(
                "SELECT count(*) AS count FROM transcript_events WHERE session_id = ? AND navigation_json IS NULL AND event_utf8_bytes > 65536",
              )
              .get(scope.sessionId),
          ).toEqual({ count: 2 });
        } finally {
          writer.close();
        }
      } else {
        for (const entry of timestamped) {
          await appendTranscriptEvent(scope, entry);
        }
      }
      await waitForSessionTranscriptProjection(scope);
      const context = readSessionTranscriptBoundedActiveContextCore(scope, {
        maxBytes: 4096,
        maxEvents: 20,
      });
      expect(context.truncated).toBe(false);
      expect(context.events.map((event) => (event as { id: string }).id)).toEqual([
        scope.sessionId,
        "thinking",
        "model",
        "kept-projection",
        "kept-branch",
        "cut",
      ]);
      expect(context.cacheTtlProjectionPrefixes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            anchorIds: ["cut"],
            entries: expect.arrayContaining([
              expect.objectContaining({ id: "full", data: full.marker }),
              expect.objectContaining({ id: "delta", data: delta.marker }),
            ]),
          }),
        ]),
      );
      const reopened = SessionManager.openBounded(scope, { maxBytes: 4096, maxEvents: 20 });
      expect(reopened.buildSessionContext()).toMatchObject({
        thinkingLevel: "high",
        model: { provider: "synthetic", modelId: "synthetic-model" },
      });
      expect(reopened.buildSessionContext().messages).toMatchObject([
        { role: "compactionSummary", summary: "Task pending" },
        { role: "custom", content: "pending validation" },
        { role: "branchSummary", summary: "Keep branch state" },
      ]);
      expect(context.serializedBytes).toBe(
        readSessionTranscriptActiveStats(scope).sizeBytes +
          Buffer.byteLength(JSON.stringify(context.events[0])) +
          1,
      );
      await appendTranscriptEvent(scope, {
        type: "custom",
        id: "damaged-cache",
        parentId: "cut",
        customType: "openclaw.cache-ttl",
        data: { prunedToolResults: "damaged", timestamp: 1 },
      });
      await appendTranscriptEvent(scope, {
        type: "compaction",
        id: "damaged-cut",
        parentId: "damaged-cache",
        firstKeptEntryId: "damaged-cut",
        summary: "Pending",
        tokensBefore: 100,
      });
      const damaged = readSessionTranscriptBoundedActiveContextCore(scope, {
        maxBytes: 4096,
        maxEvents: 20,
      });
      expect(damaged.truncated).toBe(false);
      expect(
        damaged.cacheTtlProjectionPrefixes
          ?.flatMap((prefix) => prefix.entries)
          .find((event) => (event as { id: string }).id === "damaged-cache"),
      ).toMatchObject({ data: { prunedToolResults: "damaged", timestamp: 1 } });
      expect(damaged.events.some((event) => (event as { id: string }).id === "full")).toBe(false);
    });
  },
);

it("charges the reset boundary to the same byte and event window as the reader", async () => {
  await withBoundedContextScope(async (scope) => {
    await appendTranscriptMessage(scope, {
      eventId: "kept",
      message: { role: "user", content: "pending task", timestamp: 1 },
    });
    await appendTranscriptEvent(scope, {
      type: "reset",
      id: "reset",
      parentId: "kept",
      firstKeptEntryId: "kept",
      reason: "new",
    });
    const stats = readSessionTranscriptActiveStats(scope);
    const full = readSessionTranscriptBoundedActiveContextCore(scope, {
      maxBytes: 4096,
      maxEvents: 10,
    });
    expect(full.truncated).toBe(false);
    expect(stats.eventCount).toBe(2);
    expect(full.serializedBytes).toBe(
      stats.sizeBytes + Buffer.byteLength(JSON.stringify(full.events[0])) + 1,
    );
    expect(
      readSessionTranscriptBoundedActiveContextCore(scope, {
        maxBytes: full.serializedBytes,
        maxEvents: 2,
      }).truncated,
    ).toBe(false);
    expect(
      readSessionTranscriptBoundedActiveContextCore(scope, {
        maxBytes: full.serializedBytes - 1,
        maxEvents: 2,
      }).truncated,
    ).toBe(true);
  });
});
