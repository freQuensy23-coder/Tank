import json,hashlib
from pathlib import Path
import yaml
from tank.market.normalize import oi
P=Path(__file__).parent
r=json.loads((P/'receipts.json').read_text());registry=yaml.safe_load(Path('config/registry.yaml').read_text());inst=next(x for x in registry['instruments'] if x['venue']=='bybit' and x['venue_symbol']=='WLDUSDT' and x['market_type']!='SPOT');rows=[];bindings=[]
for rec in r:
 if not rec['name'].startswith('bybit-oi-'):continue
 raw=(P/rec['path']).read_bytes();assert hashlib.sha256(raw).hexdigest()==rec['sha256'];j=json.loads(raw)
 for x in j['result']['list']:
  y=oi(inst,x,received_ts=rec['received_at'],bucket_size_s=300);y['raw_capture_ref']=str(P/rec['path']);y['raw_capture_sha256']=rec['sha256'];rows.append(y)
 bindings.append({'path':rec['path'],'sha256':rec['sha256'],'received_at':rec['received_at'],'request_params':rec['params']})
rows.sort(key=lambda x:x['ts']);assert len(rows)==2592 and len({x['ts'] for x in rows})==2592
(P/'bybit-oi-normalized-successor.json').write_text(json.dumps({'representation':'ACTUAL_LATER_ACQUIRED_HISTORICAL_POINT_SAMPLES','cadence_s':300,'cadence_source':'actual intervalTime=5min request; not bucket duration','raw_bindings':bindings,'rows':rows},indent=2)+'\n')
print(len(rows),rows[0]['ts'],rows[-1]['ts'])
