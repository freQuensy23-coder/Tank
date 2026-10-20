import {afterEach,expect,it,vi} from 'vitest';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
const observed=vi.hoisted(()=>({options:[] as any[]}));
vi.mock('@boxlite-ai/boxlite',()=>({SimpleBox:class {constructor(options:any){observed.options.push(options);}async exec(){return {exitCode:0,stdout:'',stderr:''};}async stop(){} }}));
import {sandboxFactory} from '../src/litebox';
const roots:string[]=[];
async function env(address='172.29.0.2',hosts='["postgres"]'){
  const root=await mkdtemp(join(tmpdir(),'native-ip-policy-'));roots.push(root);const run='isolated-prefix';await mkdir(join(root,run));const cutoff='2026-10-19T00:00:00Z';
  const data=JSON.stringify({schema:'tank.replay-evidence.v1',run_id:run,input_sha256:'a'.repeat(64),cutoff,cutoff_prec:'EXACT',cutoff_prec_reason:null,guest_root:'/replay/evidence',files:[],identity_key:['instrument_id','trade_id'],writer:{library:'pyarrow',version:'offline',compression:'zstd'},sealed_at:cutoff,sealed_at_prec:'COLLECTION_ONLY',sealed_at_prec_reason:null})+'\n';await writeFile(join(root,run,'manifest.json'),data);
  return {SANDBOX_IMAGE:'synthetic-no-pull',TANK_SANDBOX_REPLAY:'1',TANK_SANDBOX_ALLOWED_NET:hosts,TANK_SANDBOX_DATABASE_IPV4:address,TANK_SANDBOX_REPLAY_DB_URL:`postgresql://synthetic_readonly@${address}/tank_replay`,TANK_REPLAY_EVIDENCE_ROOT:root,TANK_REPLAY_RUN_ID:run,TANK_REPLAY_EVIDENCE_SHA256:createHash('sha256').update(data).digest('hex'),TANK_REPLAY_CUTOFF:cutoff};
}
afterEach(async()=>{observed.options.length=0;for(const root of roots.splice(0))await rm(root,{recursive:true,force:true});});
it('resolves configured database host into sole native IP rule without hostname inspection',async()=>{
  const s=await sandboxFactory({sessionId:'offline-replay',workdir:'/tmp/offline-network'} as any,await env());await s.exec('true');expect(observed.options[0].network).toEqual({mode:'enabled',allowNet:['172.29.0.2']});await s.destroy?.();
});
it('preserves unrestricted live analysis and empty replay network disable',async()=>{
  const live=await sandboxFactory({sessionId:'offline-live',workdir:'/tmp/offline-live'} as any,{SANDBOX_IMAGE:'synthetic-no-pull',TANK_SANDBOX_REPLAY:'0',TANK_SANDBOX_ALLOWED_NET:'unrestricted',TANK_SANDBOX_DB_URL:'postgresql://fake@postgres/tank'});await live.exec('true');expect(observed.options[0].network).toEqual({mode:'enabled'});await live.destroy?.();
  const replay=await sandboxFactory({sessionId:'offline-none',workdir:'/tmp/offline-none'} as any,await env('172.29.0.2','[]'));await replay.exec('true');expect(observed.options[1].network).toEqual({mode:'disabled'});await replay.destroy?.();
});
it.each(['8.8.8.8','127.0.0.1','169.254.1.1','172.15.0.2','192.169.1.2','postgres','172.29.0.2/24','172.029.0.2','::1',''])('refuses unsafe database route %s before any allocation',async address=>{
  await expect(sandboxFactory({sessionId:'invalid',workdir:'/tmp/offline-invalid'} as any,await env(address))).rejects.toThrow('private IPv4');expect(observed.options).toEqual([]);
});
it('refuses policy widening before any allocation',async()=>{await expect(sandboxFactory({sessionId:'wide',workdir:'/tmp/offline-wide'} as any,await env('172.29.0.2','["postgres","example.com"]'))).rejects.toThrow('database-only');expect(observed.options).toEqual([]);});
