import { sql } from "kysely";
import {
  executeSqliteQueryTakeFirstSync,
  iterateSqliteQuerySync,
} from "../../infra/kysely-sync.js";
import {
  getActiveTranscriptKysely,
  type CurrentTranscriptProjection,
} from "./session-accessor.sqlite-projection-read.js";
import { projectTranscriptPayloadNavigationSql } from "./session-model-context-projection.js";
import { transcriptEventReadBytesSql } from "./session-transcript-read-bytes.js";
import { transcriptEventNavigationSql } from "./transcript-payload.js";

/** State controls survive compaction independently of the summarized message prefix. */
export function readRequiredContextControls(
  projection: CurrentTranscriptProjection,
  boundaryPosition: number,
  keptPositions: readonly number[],
  beforeRawSeq?: number,
): number[] {
  const db = getActiveTranscriptKysely(projection.database);
  const navigation = transcriptEventNavigationSql("event");
  const projectedNavigation =
    /* kysely-allow-raw: imports without native metadata still return only navigation facts. */
    sql<string>`CASE WHEN event.navigation_valid = 1 THEN ${projectTranscriptPayloadNavigationSql(navigation)} ELSE ${navigation} END`;
  const rows = iterateSqliteQuerySync(
    projection.database.db,
    db
      .selectFrom("session_transcript_active_events as active")
      .innerJoin("transcript_events as event", (join) =>
        join
          .onRef("event.session_id", "=", "active.session_id")
          .onRef("event.seq", "=", "active.event_seq"),
      )
      .select([
        "active.event_seq",
        "active.active_position",
        /* kysely-allow-raw: malformed/deep legacy navigation must not hydrate an unbounded body. */
        sql<string>`CASE WHEN octet_length(${projectedNavigation}) <= 65536
          THEN ${projectedNavigation}
          ELSE '{"type":"opaque-required"}' END`.as("navigation"),
      ])
      .where("active.session_id", "=", projection.resolved.sessionId)
      .$if(beforeRawSeq !== undefined, (query) =>
        query.where("active.event_seq", "<", beforeRawSeq!),
      )
      .orderBy("active.active_position", "desc"),
  );
  const selected: number[] = [];
  const kept = new Set(keptPositions);
  let modelDone = false;
  for (const row of rows) {
    const entry = JSON.parse(row.navigation) as {
      type?: string;
      customType?: string;
      message?: { role?: string };
    };
    let required = false;
    if (
      !modelDone &&
      (entry.type === "model_change" ||
        (entry.type === "message" && entry.message?.role === "assistant"))
    ) {
      modelDone = true;
      required = true;
    }
    if (
      entry.type === "opaque-required" ||
      entry.type === "thinking_level_change" ||
      entry.type === "model_change" ||
      (entry.type === "custom" && entry.customType !== "openclaw.cache-ttl") ||
      entry.type === "session_info"
    ) {
      required = true;
    }
    if (required && row.active_position < boundaryPosition && !kept.has(row.active_position)) {
      selected.push(row.event_seq);
    }
  }
  return selected;
}

/** Required state uses the same raw-byte accounting as its bounded acquisition. */
export function readRequiredContextWindow(
  projection: CurrentTranscriptProjection,
  boundaryPosition: number | undefined,
  keptPositions: readonly number[],
  beforeRawSeq?: number,
): { sequences: number[]; eventCount: number; sizeBytes: number } {
  const sequences =
    boundaryPosition === undefined
      ? []
      : readRequiredContextControls(projection, boundaryPosition, keptPositions, beforeRawSeq);
  if (sequences.length === 0) {
    return { sequences, eventCount: 0, sizeBytes: 0 };
  }
  const db = getActiveTranscriptKysely(projection.database);
  const facts = executeSqliteQueryTakeFirstSync(
    projection.database.db,
    db
      .selectFrom("transcript_events as event")
      .select((eb) => [
        eb.fn.count<number>("event.seq").as("count"),
        /* kysely-allow-raw: required state participates in the same raw-byte fuse. */
        sql<number>`COALESCE(SUM(${transcriptEventReadBytesSql("event")} + 1), 0)`.as("bytes"),
      ])
      .where("event.session_id", "=", projection.resolved.sessionId)
      .where("event.seq", "in", sequences),
  );
  return { sequences, eventCount: facts?.count ?? 0, sizeBytes: facts?.bytes ?? 0 };
}

export function selectRetainedContextRows<T extends { event: { type: string } }>(
  candidates: T[],
  scope: "context" | "history",
  boundaryType: string | null,
): T[] {
  return scope === "context" && boundaryType === "compaction"
    ? candidates.filter(
        ({ event }) =>
          event.type === "message" ||
          event.type === "custom_message" ||
          event.type === "branch_summary",
      )
    : candidates;
}

export type ResetMessageWindow = {
  boundarySeq: number;
  keptActivePositions: number[];
  requiredControlSequences: number[];
  contextPrefixEventCount: number;
  keptMessagePositions: number[];
  contextPrefixSizeBytes: number;
  postBoundaryMessagePosition: number;
  boundaryActivePosition: number;
};
