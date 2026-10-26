import history from './history-module-config';

export default {
  ...history,
  test: {
    ...history.test,
    include: ['test/tank-continuation-projection.pg.test.ts'],
    exclude: [],
  },
};
