import { setTimeout as delay } from 'node:timers/promises';

const effect = run => Object.assign(run, {
  pipe: (...steps) => steps.reduce((work, step) => step(work), run),
  *[Symbol.iterator]() { return yield this; },
});
const catchAll = handle => work => effect(async signal => {
  try { return await work(signal); } catch (error) {
    if (signal?.aborted) throw error;
    return handle(error)(signal);
  }
});

export const Effect = {
  tryPromise: ({try: run, catch: map}) => effect(async () => {
    try { return await run(); } catch (error) { throw map(error); }
  }),
  catchAll,
  catchAllDefect: () => work => work,
  ensuring: final => work => effect(async signal => {
    try { return await work(signal); } finally { await final(signal); }
  }),
  sync: run => effect(run),
  asVoid: work => effect(async signal => { await work(signal); }),
  gen: (context, generate) => effect(async signal => {
    const iterator = generate.call(context);
    for (let step = iterator.next(); !step.done; step = iterator.next()) await step.value(signal);
  }),
  sleep: ms => effect(signal => delay(ms, undefined, {signal})),
  runFork: work => {
    const controller = new AbortController();
    const promise = work(controller.signal).catch(error => { if (!controller.signal.aborted) throw error; });
    return {controller, promise};
  },
  runPromise: work => work(),
};

export const Fiber = {
  join: fiber => () => fiber.promise,
  interrupt: fiber => async () => { fiber.controller.abort(); await fiber.promise; },
};
