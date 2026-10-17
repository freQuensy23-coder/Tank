"""Offline replay boundaries. Sealed evaluator data never enters ReplayInputs.

Runners must use supplied readers and HTTP transport, never production adapters.
A packet without captured agent/tool inputs cannot establish historical acceptance.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Awaitable, Callable

from tank.util import canonical_json, iso, parse_time


class ReplayError(ValueError):
    pass


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class ReplayInputs:
    cutoff: str
    config_version: str
    code_version: str
    registry: dict
    config: dict
    _records: tuple
    _tool_results: tuple

    def records(self, collection, *, cutoff=None):
        bound = parse_time(cutoff or self.cutoff)
        if bound > parse_time(self.cutoff):
            raise ReplayError('query exceeds replay cutoff')
        return [copy.deepcopy(row['record']) for row in self._records
                if row['collection'] == collection and parse_time(row['available_at']) <= bound]

    def tool_result(self, call_id):
        for row in self._tool_results:
            if row['call_id'] == call_id and parse_time(row['available_at']) <= parse_time(self.cutoff):
                return copy.deepcopy(row['result'])
        raise ReplayError(f'No captured pre-cutoff result for tool call {call_id}')

    async def offline_http(self, request):
        raise ReplayError(f'Network forbidden during replay: {request.method} {request.url}')


class ReplayClock:
    def __init__(self, start, cutoff):
        self.current, self.cutoff = parse_time(start), parse_time(cutoff)
        if self.current > self.cutoff:
            raise ReplayError('clock starts after cutoff')

    def advance(self, timestamp):
        value = parse_time(timestamp)
        if not self.current <= value <= self.cutoff:
            raise ReplayError('clock must advance monotonically within cutoff')
        self.current = value
        return self.current

    def now(self):
        return self.current


def load_inputs(path, settings):
    packet = json.loads(Path(path).read_text())
    if any(key in packet for key in ('outcomes', 'sealed_outcomes', 'expected_outputs')):
        raise ReplayError('sealed evaluator data must be stored outside the input packet')
    for name in ('config_version', 'code_version'):
        if packet[name] != getattr(settings, name):
            raise ReplayError(f'{name} pin mismatch')
    for name in ('registry', 'config', 'records', 'tool_results', 'cutoff'):
        if name not in packet:
            raise ReplayError(f'missing replay input {name}')
    for row in packet['records'] + packet['tool_results']:
        # Future observations must not even be passed to the runner.
        if parse_time(row['available_at']) > parse_time(packet['cutoff']):
            raise ReplayError('packet contains post-cutoff observation')
    expected = packet.get('input_sha256')
    unhashed = {k: v for k, v in packet.items() if k != 'input_sha256'}
    if expected and expected != digest(unhashed):
        raise ReplayError('input bundle digest mismatch')
    return ReplayInputs(packet['cutoff'], packet['config_version'], packet['code_version'],
                        copy.deepcopy(packet['registry']), copy.deepcopy(packet['config']),
                        tuple(copy.deepcopy(packet['records'])), tuple(copy.deepcopy(packet['tool_results'])))


def freeze(path, value):
    """Exclusive creation; identical reruns are accepted, conflicting runs fail."""
    path = Path(path)
    content = canonical_json(value) + '\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open('x') as handle:
            handle.write(content)
    except FileExistsError:
        if path.read_text() != content:
            raise ReplayError(f'immutable replay artifact differs: {path}')
    return path


async def replay_bundle(bundle_path, repo, settings, *, runner=None, output_dir=None):
    """Run an explicitly supplied offline pipeline and freeze output before evaluation.

    No default synthetic interpretation is substituted for the actual agent pipeline.
    The caller must supply a runner using captured model responses/tool results.
    """
    inputs = load_inputs(bundle_path, settings)
    if runner is None:
        runner = run_captured_tools
    output = await runner(inputs, repo)
    result = {'config_version': inputs.config_version, 'code_version': inputs.code_version,
              'cutoff': inputs.cutoff, 'input_sha256': digest(json.loads(Path(bundle_path).read_text())),
              'output': output, 'output_sha256': digest(output)}
    if output_dir:
        freeze(Path(output_dir) / 'outputs.json', result)
        freeze(Path(output_dir) / 'inputs.json', json.loads(Path(bundle_path).read_text()))
    return result


def evaluate_frozen(output_path, sealed_path):
    """Evaluator-only entry. Call after replay has returned and frozen its output."""
    output = json.loads(Path(output_path).read_text())
    if digest(output['output']) != output['output_sha256']:
        raise ReplayError('frozen output integrity failure')
    sealed = json.loads(Path(sealed_path).read_text())
    checks = []
    for assertion in sealed['assertions']:
        value = output['output']
        for component in assertion['path']:
            value = value[component]
        checks.append({'path': assertion['path'], 'passed': value == assertion['equals']})
    return {'passed': all(c['passed'] for c in checks), 'checks': checks}

async def run_captured_tools(inputs, repo):
    """Execute captured analyst tool calls through production validation and rules."""
    import httpx
    from contextlib import ExitStack
    from unittest.mock import patch
    from tank.settings import Settings
    from tank.event_tools import EventToolService
    from tank.analysis.decisions import DecisionEngine
    from tank.analysis import truth
    from tank import event_tools
    from tank.analysis import decisions, cards
    calls = inputs.config.get('replay_tool_calls')
    if not calls:
        raise ReplayError('insufficient_fixture: missing captured analyst tool calls')
    for row in inputs._records:
        if row['collection'] == 'replay.market_snapshots': continue
        await repo.put(row['collection'], row['key'], copy.deepcopy(row['record']))
    class ReplaySettings(Settings):
        def load_config(self, name):
            key = name.removesuffix('.yaml')
            return copy.deepcopy(inputs.registry if key == 'registry' else inputs.config.get(key, {}))
    settings = ReplaySettings(replay=True, config_version=inputs.config_version, code_version=inputs.code_version)
    class FixtureTruth:
        async def assess(self, event_id, token_id, cutoff=None):
            bound = cutoff or inputs.cutoff
            if parse_time(bound) > parse_time(inputs.cutoff):
                raise ReplayError('market request exceeds cutoff')
            snapshots = inputs.records('replay.market_snapshots', cutoff=bound)
            matches = [s for s in snapshots if s['token_id'] == token_id and parse_time(s['cutoff']) == parse_time(bound)]
            if len(matches) != 1:
                raise ReplayError('insufficient_fixture: missing exact market snapshot')
            result = truth.compute_market_truth(matches[0], config=inputs.config.get('thresholds', {}))
            result.update(event_id=event_id, token_id=token_id, cutoff=bound, mta_id=stable_id('MTA'), config_version=inputs.config_version, code_version=inputs.code_version)
            await repo.put('ana.market_truth', result['mta_id'], result, immutable=True)
            return result
    counter = 0
    def stable_id(prefix):
        nonlocal counter
        counter += 1
        return f'{prefix}_REPLAY_{counter:08d}'
    async def captured_http(request):
        for row in inputs._tool_results:
            if row.get('method', 'GET') == request.method and row.get('url') == str(request.url):
                return httpx.Response(row.get('status_code', 200), json=row['result'])
        raise ReplayError(f'Uncaptured replay network request: {request.method} {request.url}')
    clock = ReplayClock(min(c['available_at'] for c in calls), inputs.cutoff)
    results = []
    # The isolated CLI owns the process. These patches must never overlap live work.
    with ExitStack() as stack:
        for module in (event_tools, decisions, cards):
            if hasattr(module, 'new_id'): stack.enter_context(patch.object(module, 'new_id', stable_id))
            if hasattr(module, 'utcnow'): stack.enter_context(patch.object(module, 'utcnow', clock.now))
        async with httpx.AsyncClient(transport=httpx.MockTransport(captured_http)) as http:
            service = EventToolService(repo, settings, FixtureTruth(), DecisionEngine(settings), http=http)
            for call in calls:
                clock.advance(call['available_at'])
                result = await service.dispatch(call['tool'], copy.deepcopy(call['arguments']), copy.deepcopy(call.get('context', {})))
                results.append({'call_id': call['call_id'], 'tool': call['tool'], 'result': result})
    return {'tool_calls': results}

async def replay_fixture(repo, settings, fixture_id):
    if not __import__('re').fullmatch(r'F-\d{2}', fixture_id):
        raise ReplayError('invalid fixture identifier')
    path = settings.data_dir / 'replay' / 'inputs' / f'{fixture_id}.json'
    if not path.is_file():
        raise ReplayError(f'insufficient_fixture: no captured packet for {fixture_id}')
    bundle_id = digest(json.loads(path.read_text()))
    return await replay_bundle(path, repo, settings, output_dir=settings.data_dir/'replay'/'bundles'/bundle_id)


async def reproduce_bundle(repo, settings, sha):
    if not __import__('re').fullmatch(r'[a-f0-9]{64}', sha):
        raise ReplayError('invalid bundle digest')
    directory = settings.data_dir/'replay'/'bundles'/sha
    source = directory/'inputs.json'
    expected = json.loads((directory/'outputs.json').read_text())
    if digest(json.loads(source.read_text())) != sha:
        raise ReplayError('bundle input integrity failure')
    actual = await replay_bundle(source, repo, settings)
    if actual != expected:
        raise ReplayError('bundle reproduction differs from immutable output')
    return {'reproduced': True, 'input_sha256': sha, 'output_sha256': actual['output_sha256']}
