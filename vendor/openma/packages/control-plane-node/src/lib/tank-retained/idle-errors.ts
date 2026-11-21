export class RetainedIdleUnconfirmed extends Error {
  constructor(code: 'retained_idle_unconfirmed' | 'retained_idle_timeout', cause?: unknown) {
    super(code, { cause });
    this.name = 'RetainedIdleUnconfirmed';
  }
}

export class RetainedIdlePendingTimeout extends RetainedIdleUnconfirmed {
  constructor() {
    super('retained_idle_timeout');
  }
}

export class RetainedIdleAttemptsExhausted extends RetainedIdleUnconfirmed {
  constructor(cause: unknown) {
    super('retained_idle_unconfirmed', cause);
  }
}

export class RetainedExecutionFenceLost extends Error {
  constructor(message = 'Retained Session execution fence lost') {
    super(message);
    this.name = 'RetainedExecutionFenceLost';
  }
}

export class RetainedResumeFailed extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = 'RetainedResumeFailed';
  }
}
