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
