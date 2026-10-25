export const requiredSqlCases = Object.freeze([
  'actual SQL coordinator recovers once, fences a concurrent owner and preserves FIFO after durable reopen',
  ...['unrelated', 'effects', 'archived', 'interrupted', 'deadline', 'attempt-limit'].map(reason => `actual SQL recovery refuses ${reason} without rewriting failed history`),
  'actual SQL interrupted recovered execution stays cancelled and fresh input works',
  'actual SQL temporary preflight retries preserve original deadline and exactly-once result',
  'actual SQL expired pre-model owner at attempt cap refunds and reclaims without deadline extension',
  ...['effects', 'interrupt', 'deadline'].map(reason => `actual SQL expired recovery never refunds or revives ${reason}`),
  'actual SQL transfer timeout producer settles and retries original input after durable reopen',
  'actual SQL expired-owner sweep preserves 105 recoverable sessions beyond one batch and refuses unsafe controls',
  'actual SQL failed sibling RX RY consume both results then one model phase after reopen',
  'actual SQL unrelated failed sibling is not classified as pending image recovery',
]);
