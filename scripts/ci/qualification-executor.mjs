import assert from 'node:assert/strict';

export function validateQualificationExecutor(env) {
  assert.equal(env.GITHUB_ACTIONS, 'true');
  assert.equal(env.RUNNER_ENVIRONMENT, 'github-hosted');
  assert.equal(env.GITHUB_EVENT_NAME, 'workflow_dispatch');
  assert.equal(env.GITHUB_REPOSITORY, 'freQuensy23-coder/Tank');
  assert.equal(env.GITHUB_ACTOR, 'freQuensy23-coder');
  assert.match(env.GITHUB_SHA ?? '', /^[a-f0-9]{40}$/);
  assert.match(env.GITHUB_REF ?? '', /^refs\/heads\/[^\s]+$/);
  return 'github-hosted';
}

export async function qualificationExecutor(env = process.env) {
  return validateQualificationExecutor(env);
}
