export class RetainedIdleUnconfirmed extends Error {
  constructor(code: 'retained_idle_unconfirmed' | 'retained_idle_timeout', cause?: unknown) {
    super(code, { cause });
    this.name = 'RetainedIdleUnconfirmed';
  }
}

export class RetainedExecutionFenceLost extends Error {
  constructor() {
    super('Retained Session execution fence lost');
    this.name = 'RetainedExecutionFenceLost';
  }
}

export class RetainedResumeFailed extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = 'RetainedResumeFailed';
  }
}
