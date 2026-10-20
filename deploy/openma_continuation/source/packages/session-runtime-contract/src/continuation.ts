/** Derive callback continuation and confirmed input progress from the native journal. */
import type { SentSessionEvent, SessionEventView, SessionStopReason } from '@open-managed-agents/domain/sessions';
import type { OrderedSessionEvent, ExecutionProgressReceipt } from './history';
import type { SessionExecutionState } from './coordination';
export interface ContinuationExecution {
  id: string;
  laneId: string;
  state: SessionExecutionState;
  revision: number;
  events: SentSessionEvent[];
  attemptCount: number;
  interruptRequestedAt: string | null;
}
export interface ContinuationSnapshot {
  invalidReason?: string;
  revision: number;
  orderedEvents: OrderedSessionEvent[];
  executions: ContinuationExecution[];
}
export interface ContinuationReadyPlan {
  mode: 'model' | 'intake' | 'noop' | 'finalize' | 'failure';
  historyEvents: SessionEventView[];
  remaining: string[];
  phaseInputIds: string[];
  consumedInputIds: string[];
  remainingInputIds: string[];
  previousReceiptId: string | null;
  phaseStartEventId?: string;
  recoveredStopReason?: SessionStopReason;
  failure?: string;
}
export type ContinuationPlan = {
  mode: 'blocked';
  reason: string;
} | ContinuationReadyPlan;
export class ContinuationProtocolError extends Error {
  readonly name = 'ContinuationProtocolError';
}
export interface VerifiedProgress {
  entry: OrderedSessionEvent;
  receipt: ExecutionProgressReceipt;
}
// A loaded native snapshot is immutable for the duration of a claim/projection decision.
const snapshotCache = new WeakMap<ContinuationSnapshot, {
  progress: Map<string, VerifiedProgress[]>;
  phases?: Phase[];
  results?: SentSessionEvent[];
  groups: Map<string, Group[]>;
}>();
function snapshotDerivedState(snapshot: ContinuationSnapshot) {
  let value = snapshotCache.get(snapshot);
  if (!value) {
    value = { progress: new Map(), groups: new Map() };
    snapshotCache.set(snapshot, value);
  }
  return value;
}
export const actionableInputs = (row: ContinuationExecution) => row.events.filter(e => e.type !== 'system.message');
const eventLaneId = (e: SessionEventView) => 'sessionThreadId' in e ? e.sessionThreadId ?? 'sthr_primary' : 'sthr_primary';
export const resultCallId = (e: SentSessionEvent): string | undefined => e.type === 'user.custom_tool_result' ? e.customToolUseId : undefined;
const sameResult = (a: SentSessionEvent, b: SentSessionEvent) => JSON.stringify(a.type === 'user.custom_tool_result' ? [a.content ?? [], a.isError ?? false] : a) === JSON.stringify(b.type === 'user.custom_tool_result' ? [b.content ?? [], b.isError ?? false] : b);
const sameUniqueIds = (a: string[], b: string[]) => a.length === b.length && new Set(a).size === a.length && a.every(id => b.includes(id));
const assertTrustedJournalOrder = (s: ContinuationSnapshot) => {
  const seen = new Set<string>();
  let revision = -1, index = -1;
  for (const entry of s.orderedEvents) {
    if (!Number.isSafeInteger(entry.position.revision) || !Number.isSafeInteger(entry.position.index) || entry.position.revision < 1 || entry.position.index < 0 || entry.position.revision < revision ||
      (entry.position.revision === revision && entry.position.index <= index) || seen.has(entry.event.id))
      throw new ContinuationProtocolError('Untrusted continuation journal order');
    revision = entry.position.revision;
    index = entry.position.index;
    seen.add(entry.event.id);
  }
};
/** Every progress record is private, fenced, monotone and atomically confirmed by yield. */
export function verifiedProgress(snapshot: ContinuationSnapshot, row: ContinuationExecution): VerifiedProgress[] {
  const cached = snapshotDerivedState(snapshot).progress.get(row.id);
  if (cached)
    return cached;
  const original = actionableInputs(row).map(e => e.id);
  const result: VerifiedProgress[] = [];
  let previous: string | null = null, consumed: string[] = [];
  for (const entry of snapshot.orderedEvents) {
    const p = entry.executionMetadata?.progress;
    if (!p)
      continue;
    if (entry.executionId !== row.id)
      continue;
    const metadata = entry.executionMetadata!;
    const start = snapshot.orderedEvents.find(e => e.event.id === p.phaseStartEventId);
    const terminal = snapshot.orderedEvents.find(e => e.event.id === p.terminalEventId);
    const pos = snapshot.orderedEvents.indexOf(entry);
    if (p.version !== 1 || p.laneId !== row.laneId || eventLaneId(entry.event) !== row.laneId || entry.event.type !== 'session.status_idle' ||
      p.previousReceiptId !== previous || !Array.isArray(p.consumedInputIds) || !Array.isArray(p.remainingInputIds) || !Array.isArray(p.phaseInputIds) ||
      !sameUniqueIds(p.consumedInputIds, [...consumed, ...p.phaseInputIds]) || p.phaseInputIds.length === 0 ||
      !p.consumedInputIds.every(id => original.includes(id)) || !sameUniqueIds(p.remainingInputIds, original.filter(id => !p.consumedInputIds.includes(id))) ||
      p.remainingInputIds.length === 0 || !start || !terminal || start.executionId !== row.id || terminal.executionId !== row.id ||
      eventLaneId(start.event) !== row.laneId || eventLaneId(terminal.event) !== row.laneId || terminal.event.type !== 'session.status_idle' ||
      snapshot.orderedEvents.indexOf(start) > snapshot.orderedEvents.indexOf(terminal) || snapshot.orderedEvents.indexOf(terminal) >= pos ||
      typeof metadata.attemptId !== 'string' || metadata.attemptId.length === 0 || metadata.attemptId !== p.attemptId || metadata.generation !== p.generation || !Number.isSafeInteger(p.generation) || p.generation < 1 ||
      terminal.executionMetadata?.attemptId !== p.attemptId || terminal.executionMetadata?.generation !== p.generation ||
      JSON.stringify(terminal.event.stopReason) !== JSON.stringify(entry.event.stopReason))
      throw new ContinuationProtocolError('Invalid private execution progress chain');
    consumed = [...p.consumedInputIds];
    previous = entry.event.id;
    result.push({ entry, receipt: p });
  }
  snapshotDerivedState(snapshot).progress.set(row.id, result);
  return result;
}
function progressByExecution(snapshot: ContinuationSnapshot) {
  return new Map(snapshot.executions.map(row => [row.id, verifiedProgress(snapshot, row)]));
}
function lastProgress(snapshot: ContinuationSnapshot, row: ContinuationExecution) {
  return verifiedProgress(snapshot, row).at(-1);
}
function remainingInputs(snapshot: ContinuationSnapshot, row: ContinuationExecution) {
  const consumed = lastProgress(snapshot, row)?.receipt.consumedInputIds ?? [];
  return actionableInputs(row).filter(e => !consumed.includes(e.id));
}
interface Phase {
  owner: string;
  inputIds: string[];
  start: number;
  end: number;
  terminal: OrderedSessionEvent;
}
function committedPhases(snapshot: ContinuationSnapshot): Phase[] {
  const cached = snapshotDerivedState(snapshot).phases;
  if (cached)
    return cached;
  const phases: Phase[] = [];
  for (const row of snapshot.executions) {
    const progress = verifiedProgress(snapshot, row);
    for (const { entry, receipt } of progress)
      phases.push({
        owner: row.id, inputIds: receipt.phaseInputIds,
        start: snapshot.orderedEvents.findIndex(e => e.event.id === receipt.phaseStartEventId), end: snapshot.orderedEvents.indexOf(entry),
        terminal: snapshot.orderedEvents.find(e => e.event.id === receipt.terminalEventId)!
      });
    if (row.state !== 'completed')
      continue;
    const after = progress.length ? snapshot.orderedEvents.indexOf(progress.at(-1)!.entry) : -1;
    const output = snapshot.orderedEvents.filter((e, i) => i > after && e.executionId === row.id && !e.executionMetadata?.progress);
    const terminal = output.findLast(e => e.event.type === 'session.status_idle');
    if (!terminal)
      continue;
    phases.push({
      owner: row.id, inputIds: remainingInputs(snapshot, row).map(e => e.id),
      start: snapshot.orderedEvents.indexOf(output[0]!), end: snapshot.orderedEvents.indexOf(terminal), terminal
    });
  }
  snapshotDerivedState(snapshot).phases = phases;
  return phases;
}
interface Group {
  ids: string[];
  declaration: OrderedSessionEvent;
  position: number;
  cancelled: boolean;
  closed: boolean;
  interruptPosition?: number;
}
function groups(snapshot: ContinuationSnapshot, laneId: string): Group[] {
  const cached = snapshotDerivedState(snapshot).groups.get(laneId);
  if (cached)
    return cached;
  const phases = committedPhases(snapshot);
  const groups: Group[] = [];
  for (const phase of phases) {
    const e = phase.terminal.event;
    if (eventLaneId(e) !== laneId || e.type !== 'session.status_idle' || e.stopReason.type !== 'requires_action')
      continue;
    const ids = e.stopReason.eventIds;
    const uses = snapshot.orderedEvents.slice(phase.start, phase.end + 1).filter(x => x.executionId === phase.owner && x.event.type === 'agent.custom_tool_use');
    if (uses.length === 0)
      continue; // An intake receipt never invents a new group.
    if (!sameUniqueIds(ids, uses.map(x => x.event.id)))
      throw new ContinuationProtocolError('Invalid custom-tool group declaration');
    const position = snapshot.orderedEvents.indexOf(phase.terminal);
    const interruptPosition = snapshot.orderedEvents.findIndex((x, i) => i > position && x.event.type === 'user.interrupt' &&
      (x.event.sessionThreadId == null || x.event.sessionThreadId === laneId));
    const interrupted = interruptPosition >= 0;
    const closed = phases.some(next => next.start > position && next.inputIds.some(id => {
      const input = snapshot.executions.flatMap(actionableInputs).find(e => e.id === id);
      return input && ids.includes(resultCallId(input) ?? '');
    }) && snapshot.orderedEvents.slice(next.start, next.end + 1).some(x => x.executionId === next.owner && x.event.type.startsWith('agent.')));
    const globallySupplied = new Set(phases.flatMap(committed => committed.inputIds).map(id => resultCallId(snapshot.executions.flatMap(actionableInputs).find(input => input.id === id)!)).filter((id): id is string => id !== undefined));
    const failedDependency = !closed && snapshot.executions.some(owner => owner.laneId === laneId && ['failed', 'cancelled'].includes(owner.state) &&
      actionableInputs(owner).some(input => ids.includes(resultCallId(input) ?? '') && !globallySupplied.has(resultCallId(input)!)));
    const cancelled = interrupted || failedDependency;
    groups.push({
      ids: [...ids], declaration: phase.terminal, position, cancelled, closed,
      ...(cancelled ? { interruptPosition: interrupted ? interruptPosition : snapshot.orderedEvents.length } : {})
    });
  }
  snapshotDerivedState(snapshot).groups.set(laneId, groups);
  return groups;
}
function committedResults(snapshot: ContinuationSnapshot): SentSessionEvent[] {
  const cached = snapshotDerivedState(snapshot).results;
  if (cached)
    return cached;
  const ids = new Set(committedPhases(snapshot).flatMap(phase => phase.inputIds));
  const results = snapshot.executions.flatMap(actionableInputs).filter(e => ids.has(e.id) && resultCallId(e) !== undefined);
  snapshotDerivedState(snapshot).results = results;
  return results;
}
function earlierNewWork(snapshot: ContinuationSnapshot, row: ContinuationExecution) {
  const prior = snapshot.executions.slice(0, snapshot.executions.findIndex(e => e.id === row.id));
  return prior.some(other => other.laneId === row.laneId && ['queued', 'running'].includes(other.state) &&
    remainingInputs(snapshot, other).some(e => resultCallId(e) === undefined));
}
export function planSessionContinuation(snapshot: ContinuationSnapshot, currentId: string): ContinuationPlan {
  try {
    if (snapshot.invalidReason)
      throw new ContinuationProtocolError(snapshot.invalidReason);
    return planContinuation(snapshot, currentId);
  }
  catch (error) {
    if (!(error instanceof ContinuationProtocolError))
      throw error;
    const owner = snapshot.executions.find(row => row.id === currentId);
    if (!owner)
      return { mode: 'blocked', reason: 'missing_execution' };
    return {
      mode: 'failure', failure: 'Original execution ' + currentId + ' cannot safely continue: ' + error.message,
      historyEvents: [], remaining: [], phaseInputIds: actionableInputs(owner).map(e => e.id),
      consumedInputIds: [], remainingInputIds: [], previousReceiptId: null
    };
  }
}
function planContinuation(snapshot: ContinuationSnapshot, currentId: string): ContinuationPlan {
  assertTrustedJournalOrder(snapshot);
  const row = snapshot.executions.find(e => e.id === currentId);
  if (!row)
    return { mode: 'blocked', reason: 'missing_execution' };
  if (row.state === 'failed' || row.state === 'cancelled' || row.interruptRequestedAt !== null)
    return { mode: 'blocked', reason: 'interrupted_or_terminal_failure' };
  const progress = lastProgress(snapshot, row), already = progress?.receipt.consumedInputIds ?? [], remaining = remainingInputs(snapshot, row);
  if (!remaining.length)
    return { mode: 'blocked', reason: 'invalid_empty_remaining_execution' };
  const after = progress ? snapshot.orderedEvents.indexOf(progress.entry) : -1;
  const output = snapshot.orderedEvents.filter((e, i) => i > after && e.executionId === row.id && !e.executionMetadata?.progress);
  const first = output[0];
  // Recover phase membership from the immutable prefix at its first output, never from newly accepted inputs.
  const phaseSnapshot = first ? { ...snapshot, orderedEvents: snapshot.orderedEvents.slice(0, snapshot.orderedEvents.indexOf(first)) } : snapshot;
  const known = groups(phaseSnapshot, row.laneId), open = known.filter(g => !g.closed && !g.cancelled);
  if (open.length > 1)
    return { mode: 'blocked', reason: 'overlapping_open_tool_groups' };
  const group = open[0];
  const results = committedResults(phaseSnapshot);
  let phase: SentSessionEvent[] = [];
  let mode: ContinuationReadyPlan['mode'] = 'model';
  let pending: string[] = [];
  const knownCall = (id: string) => known.some(g => g.ids.includes(id));
  if (group) {
    phase = remaining.filter(e => {
      const id = resultCallId(e);
      return id !== undefined && (group.ids.includes(id) || knownCall(id));
    });
    if (!phase.length)
      return { mode: 'blocked', reason: 'new_work_waits_for_declared_callbacks' };
    const received = new Map<string, SentSessionEvent>();
    for (const e of [...results, ...phase]) {
      const id = resultCallId(e)!;
      const prev = received.get(id);
      if (prev && !sameResult(prev, e))
        throw new ContinuationProtocolError('Conflicting result for declared custom tool ' + id);
      received.set(id, e);
    }
    pending = group.ids.filter(id => !received.has(id));
    mode = pending.length ? 'intake' : 'model';
  }
  else {
    const unknown = remaining.find(e => resultCallId(e) !== undefined && !knownCall(resultCallId(e)!));
    if (unknown)
      throw new ContinuationProtocolError('Result refers to an unknown or failed custom-tool group');
    const hasUser = remaining.some(e => resultCallId(e) === undefined);
    if (hasUser && earlierNewWork(snapshot, row))
      return { mode: 'blocked', reason: 'earlier_new_work_remains_fifo' };
    phase = remaining;
    mode = hasUser ? 'model' : 'noop';
    for (const e of phase) {
      const id = resultCallId(e);
      if (id === undefined)
        continue;
      const original = results.find(p => resultCallId(p) === id);
      const cancelled = known.some(g => g.cancelled && g.ids.includes(id));
      if (!cancelled && (!original || !sameResult(original, e)))
        throw new ContinuationProtocolError('Conflicting previously consumed custom-tool result');
    }
  }
  const phaseInputIds = phase.map(e => e.id), consumedInputIds = actionableInputs(row).filter(e => already.includes(e.id) || phaseInputIds.includes(e.id)).map(e => e.id);
  const plan: ContinuationReadyPlan = {
    mode, remaining: pending, phaseInputIds, consumedInputIds,
    remainingInputIds: actionableInputs(row).filter(e => !consumedInputIds.includes(e.id)).map(e => e.id),
    previousReceiptId: progress?.entry.event.id ?? null, historyEvents: [], ...(first ? { phaseStartEventId: first.event.id } : {})
  };
  if (output.length) {
    const terminal = output.findLast(e => e.event.type === 'session.status_idle');
    const error = output.findLast(e => e.event.type === 'session.error' && e.event.error.retryStatus === 'terminal');
    if (error) {
      plan.mode = 'failure';
      plan.failure = 'Original execution already has terminal failure: ' + (error.event.type === 'session.error' ? error.event.error.message : '');
    }
    else if (terminal?.event.type === 'session.status_idle') {
      if (['end_turn', 'requires_action'].includes(terminal.event.stopReason.type)) {
        plan.mode = 'finalize';
        plan.recoveredStopReason = terminal.event.stopReason;
      }
      else {
        plan.mode = 'failure';
        plan.failure = 'Original execution has an unsuccessful committed terminal receipt';
      }
    }
    else if (output.some(e => e.event.type.startsWith('agent.'))) {
      const start = output.findLast(e => e.event.type === 'span.model_request_start');
      const end = start && output.findLast(e => e.event.type === 'span.model_request_end' && e.event.modelRequestStartId === start.event.id);
      const latestSpan = start ? output.slice(output.indexOf(start)) : output;
      const localUses = output.filter(e => e.event.type === 'agent.tool_use' || e.event.type === 'agent.mcp_tool_use');
      const latestLocalUses = latestSpan.filter(e => e.event.type === 'agent.tool_use' || e.event.type === 'agent.mcp_tool_use');
      const localComplete = localUses.every(use => output.some(e => (e.event.type === 'agent.tool_result' && e.event.toolUseId === use.event.id) || (e.event.type === 'agent.mcp_tool_result' && e.event.mcpToolUseId === use.event.id)));
      if (end?.event.type === 'span.model_request_end' && end.event.isError === false && localComplete) {
        const custom = latestSpan.filter(e => e.event.type === 'agent.custom_tool_use').map(e => e.event.id);
        if (custom.length || latestLocalUses.length === 0) {
          plan.mode = 'finalize';
          plan.recoveredStopReason = custom.length ? { type: 'requires_action', eventIds: custom } : { type: 'end_turn' };
        }
        // Completed builtin results legitimately continue the same original model turn.
      }
      else {
        plan.mode = 'failure';
        plan.failure = 'Recovery refused ambiguous committed output for original execution ' + row.id + '; no complete model/tool terminal boundary';
      }
    }
  }
  plan.historyEvents = projectConsumedHistory(snapshot, row.id, phaseInputIds);
  return plan;
}
/** Inputs move only in this model view, at trusted causal boundaries; the journal is unchanged. */
export function projectConsumedHistory(snapshot: ContinuationSnapshot, currentId: string, phaseInputIds: string[]): SessionEventView[] {
  const inputs = new Map(snapshot.executions.flatMap(r => r.events.map(e => [e.id, e] as const)));
  const insertion = new Map<string, string[]>(), tail: string[] = [];
  const allowed = new Set<OrderedSessionEvent>();
  const current = snapshot.executions.find(r => r.id === currentId)!;
  const phases = committedPhases(snapshot);
  for (const p of phases) {
    const boundary = snapshot.orderedEvents[p.start]?.event.id;
    if (boundary)
      insertion.set(boundary, [...(insertion.get(boundary) ?? []), ...p.inputIds]);
    for (const e of snapshot.orderedEvents.slice(p.start, p.end + 1))
      if (e.executionId === p.owner)
        allowed.add(e);
  }
  const progress = lastProgress(snapshot, current), after = progress ? snapshot.orderedEvents.indexOf(progress.entry) : -1;
  const own = snapshot.orderedEvents.filter((e, i) => i > after && e.executionId === currentId && !e.executionMetadata?.progress);
  if (own[0])
    insertion.set(own[0].event.id, [...(insertion.get(own[0].event.id) ?? []), ...phaseInputIds]);
  else
    tail.push(...phaseInputIds);
  own.forEach(e => allowed.add(e));
  const cancelled = groups(snapshot, current.laneId).filter(g => g.cancelled);
  const droppedCalls = new Set(cancelled.flatMap(g => g.ids.filter(id => !phases.some(p => p.end < (g.interruptPosition ?? -1) &&
    p.inputIds.some(input => resultCallId(inputs.get(input)!) === id)))));
  const emitted = new Set<string>(), seenResults = new Map<string, SentSessionEvent>(), out: SessionEventView[] = [];
  const addInput = (id: string) => {
    const e = inputs.get(id);
    if (!e || emitted.has(id))
      return;
    const owner = snapshot.executions.find(row => row.events.some(input => input.id === id));
    for (const system of owner?.events.filter(input => input.type === 'system.message') ?? [])
      if (!emitted.has(system.id)) {
        emitted.add(system.id);
        out.push(system);
      }
    emitted.add(id);
    const call = resultCallId(e);
    if (call !== undefined) {
      if (droppedCalls.has(call))
        return;
      const old = seenResults.get(call);
      if (old) {
        if (!sameResult(old, e))
          throw new ContinuationProtocolError('Conflicting result history');
        return;
      }
      seenResults.set(call, e);
    }
    out.push(e);
  };
  for (const entry of snapshot.orderedEvents) {
    for (const id of insertion.get(entry.event.id) ?? [])
      addInput(id);
    if (inputs.has(entry.event.id) || entry.executionMetadata?.progress)
      continue;
    if (entry.executionId !== undefined && !allowed.has(entry))
      continue;
    if (droppedCalls.has(entry.event.id) && entry.event.type === 'agent.custom_tool_use')
      continue;
    out.push(entry.event);
  }
  tail.forEach(addInput);
  return out;
}
