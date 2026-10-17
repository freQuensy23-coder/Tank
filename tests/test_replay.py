import json
from pathlib import Path
import pytest
from tank.replay import ReplayError, ReplayClock, replay_bundle, load_inputs, evaluate_frozen
from tank.settings import Settings
from tank.storage import MemoryRepository

@pytest.fixture
def packet(tmp_path):
    p=tmp_path/'input.json'
    p.write_text(json.dumps(dict(cutoff='2026-09-24T12:00:00Z',config_version='test',code_version='test',registry={},config={},records=[],tool_results=[])))
    return p

def settings():return Settings(config_version='test',code_version='test',replay=True)

async def test_sealed_outcomes_only_after_freeze_and_immutable(packet,tmp_path):
    async def runner(inputs,repo):
        assert not hasattr(inputs,'outcomes')
        return {'action':'WAIT'}
    out=tmp_path/'frozen'
    await replay_bundle(packet,MemoryRepository(),settings(),runner=runner,output_dir=out)
    sealed=tmp_path/'sealed.json';sealed.write_text(json.dumps({'assertions':[{'path':['action'],'equals':'WAIT'}]}))
    assert evaluate_frozen(out/'outputs.json',sealed)['passed']
    async def changed(inputs,repo):return {'action':'START'}
    with pytest.raises(ReplayError,match='immutable'):
        await replay_bundle(packet,MemoryRepository(),settings(),runner=changed,output_dir=out)

async def test_default_runs_validated_tool(packet):
    data=json.loads(packet.read_text())
    data['config']['replay_tool_calls']=[dict(call_id='one',tool='get_item',arguments={'item_id':'item'},available_at=data['cutoff'])]
    data['records']=[dict(collection='ingest.source_items',key='item',available_at=data['cutoff'],record={'item_id':'item','text':'Synthetic test item'})]
    packet.write_text(json.dumps(data))
    result=await replay_bundle(packet,MemoryRepository(),settings())
    assert result['output']['tool_calls'][0]['result']['text']=='Synthetic test item'

async def test_missing_captures_explicit(packet):
    with pytest.raises(ReplayError,match='insufficient_fixture'):
        await replay_bundle(packet,MemoryRepository(),settings())

@pytest.mark.parametrize('change',[{'outcomes':{}},{'code_version':'wrong'},{'records':[dict(available_at='2027-01-01T00:00:00Z')]}])
def test_reject_contaminated_or_unpinned(packet,change):
    data=json.loads(packet.read_text());data.update(change);packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError):load_inputs(packet,settings())

def test_clock_and_query_bounds(packet):
    inputs=load_inputs(packet,settings())
    with pytest.raises(ReplayError):inputs.records('x',cutoff='2027-01-01T00:00:00Z')
    clock=ReplayClock('2026-09-24T10:00:00Z',inputs.cutoff)
    with pytest.raises(ReplayError):clock.advance('2026-09-24T09:00:00Z')

def test_manifest_honest():
    manifest=json.loads((Path(__file__).parent/'fixtures/manifest.json').read_text())
    assert len(manifest['fixtures'])==27
    assert all(f['readiness']=='INCOMPLETE_PACKET' for f in manifest['fixtures'])
    assert manifest['fixtures'][15]['known_conflict']
