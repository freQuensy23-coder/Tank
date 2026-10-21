import asyncio,json,hashlib,shutil
from datetime import datetime,timezone
from pathlib import Path
import httpx
P=Path(__file__).parent;records=[]
def now():return datetime.now(timezone.utc).isoformat()
async def main():
 async with httpx.AsyncClient(timeout=25,trust_env=False) as c:
  async def get(name,url,params):
   if shutil.disk_usage(P).free<8*1024**3:raise RuntimeError('DISK_FLOOR_8GiB')
   started=now();z=await c.get(url,params=params);body=z.content;f=P/(name+'.body');f.write_bytes(body);r={'name':name,'url':url,'params':params,'started_at':started,'received_at':now(),'status':z.status_code,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'path':f.name,'headers':dict(z.headers)};records.append(r);(P/'resume-receipts.json').write_text(json.dumps(records,indent=2)+'\n');print(name,z.status_code,len(body),flush=True);await asyncio.sleep(.75);return z
  # Each workload serial; documented public endpoint rates conservatively bounded.
  after=json.load(open(P/'acquisition-status.json'))['okx']['resume_request']['after']
  for page in range(130,400):
   z=await get('okx-history-%03d'%page,'https://www.okx.com/api/v5/market/history-trades',{'instId':'WLD-USDT','type':'1','after':after,'limit':'100'});j=z.json();rs=j.get('data',[])
   if j.get('code')!='0' or not rs:break
   if min(int(x['ts']) for x in rs)<=1792139400000:break
   nxt=min(rs,key=lambda x:int(x['tradeId']))['tradeId']
   if nxt==after:break
   after=nxt
  after=None
  for page in range(60):
   params={'limit':1000}
   if after is not None:params['after']=after
   z=await get('coinbase-history-%03d'%page,'https://api.exchange.coinbase.com/products/WLD-USD/trades',params)
   if z.status_code!=200:break
   rs=z.json()
   if not isinstance(rs,list) or not rs:break
   if min(datetime.fromisoformat(x['time'].replace('Z','+00:00')).timestamp() for x in rs)<=1792139400:break
   nxt=z.headers.get('cb-after')
   if not nxt or nxt==after:break
   after=nxt
  initial=json.load(open(P/'kraken-probe.body'));since=str(initial['result']['last'])
  for page in range(1,40):
   z=await get('kraken-history-%03d'%page,'https://api.kraken.com/0/public/Trades',{'pair':'WLDUSD','since':since,'count':1000});j=z.json()
   if j.get('error'):break
   result=j.get('result',{});rs=result.get('WLDUSD',[])
   if not rs:break
   if max(float(x[2]) for x in rs)>=1792153800:break
   nxt=str(result.get('last'))
   if nxt==since:break
   since=nxt
 print('DONE',flush=True)
asyncio.run(main())
