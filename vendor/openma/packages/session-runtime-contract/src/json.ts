export class RuntimeJsonDocumentError extends Error {
  constructor() { super('Invalid session runtime JSON document'); }
}
function invalidRuntimeJson(): never { throw new RuntimeJsonDocumentError(); }

export function decodeRuntimeJson(document: string): unknown {
  let value: unknown;
  try { value = JSON.parse(document); }
  catch { return invalidRuntimeJson(); }
  function whitespace(start: number): number {
    let index = start;
    while (index < document.length && /\s/.test(document[index])) index++;
    return index;
  }
  function stringEnd(start: number): number {
    let index = start + 1;
    while (index < document.length) {
      if (document[index] === '"') return index + 1;
      if (document[index] === '\\') index++;
      index++;
    }
    return invalidRuntimeJson();
  }
  function visit(start: number): number {
    let index = whitespace(start);
    const kind = document[index];
    if (kind === '"') return stringEnd(index);
    if (kind === '{') {
      index = whitespace(index + 1);
      const keys = new Set<string>();
      if (document[index] === '}') return index + 1;
      while (index < document.length) {
        const end = stringEnd(index);
        const key: unknown = JSON.parse(document.slice(index, end));
        if (typeof key !== 'string' || keys.has(key)) return invalidRuntimeJson();
        keys.add(key);
        index = whitespace(end);
        index = whitespace(visit(index + 1));
        if (document[index] === '}') return index + 1;
        index = whitespace(index + 1);
      }
      return invalidRuntimeJson();
    }
    if (kind === '[') {
      index = whitespace(index + 1);
      if (document[index] === ']') return index + 1;
      while (index < document.length) {
        index = whitespace(visit(index));
        if (document[index] === ']') return index + 1;
        index = whitespace(index + 1);
      }
      return invalidRuntimeJson();
    }
    const tokenStart = index;
    while (index < document.length && !/[\s,}\]]/.test(document[index])) index++;
    const token = document.slice(tokenStart, index);
    if (Number(token) === 0 && /[1-9]/.test(token.split(/[eE]/, 1)[0])) return invalidRuntimeJson();
    return index;
  }
  if (whitespace(visit(0)) !== document.length) return invalidRuntimeJson();
  function finite(input: unknown): void {
    if (typeof input === 'number' && !Number.isFinite(input)) invalidRuntimeJson();
    if (Array.isArray(input)) {
      for (const child of input) finite(child);
    } else if (input !== null && typeof input === 'object') {
      for (const child of Object.values(input)) finite(child);
    }
  }
  finite(value);
  return value;
}
