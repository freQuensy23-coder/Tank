import { afterEach, expect, it, vi } from 'vitest';
const state=vi.hoisted(()=>({options:[] as any[],commands:[] as any[]}));
vi.mock('@boxlite-ai/boxlite',()=>({SimpleBox:class{
 constructor(opts:any){state.options.push(opts);}
 async exec(...args:any[]){state.commands.push(args);return {exitCode:0,stdout:'ok',stderr:''};}
 async stop(){} async copyIn(){} async copyOut(){}
}}));
import {sandboxFactory} from '../src/litebox';
afterEach(()=>{state.options.length=0;state.commands.length=0;});
it('analysis enables internet and injects only curated read-only credentials',async()=>{
 const s=await sandboxFactory({sessionId:'tank-test',workdir:'/tmp/tank-overlay-analysis'} as any,{
  SANDBOX_IMAGE:'test-python312',TANK_SANDBOX_ALLOWED_NET:'unrestricted',TANK_SANDBOX_REPLAY:'0',
  TANK_SANDBOX_DB_URL:'postgresql://readonly@postgres/tank',TANK_SANDBOX_OPENROUTER_API_KEY:'synthetic-openrouter',TANK_SANDBOX_JEV_PROVIDER:'openrouter',
  TANK_SANDBOX_TYPESAFE_API_KEY:'wrong-provider-must-not-leak',API_KEY:'must-not-leak',TELEGRAM_TOKEN:'must-not-leak',CODEX_AUTH:'must-not-leak'
 });
 await s.exec('true');expect(state.options[0].network).toEqual({mode:'enabled'});
 const env=state.commands.at(-1)[2];expect(env).toEqual({TANK_DB_URL:'postgresql://readonly@postgres/tank',OPENROUTER_API_KEY:'synthetic-openrouter',TANK_JEV_PROVIDER:'openrouter'});
 expect(state.options[0].image).toBe('test-python312');await s.destroy?.();
});
it('replay allows only explicit database host and omits outside API secrets',async()=>{
 const s=await sandboxFactory({sessionId:'tank-replay-test',workdir:'/tmp/tank-overlay-replay'} as any,{
  TANK_SANDBOX_ALLOWED_NET:'["postgres"]',TANK_SANDBOX_REPLAY:'1',TANK_SANDBOX_REPLAY_DB_URL:'postgresql://readonly@postgres/tank_replay',
  TANK_SANDBOX_TYPESAFE_API_KEY:'must-not-leak',TANK_SANDBOX_OPENROUTER_API_KEY:'must-not-leak',TANK_SANDBOX_JEV_PROVIDER:'openrouter',TANK_SANDBOX_ETHERSCAN_API_KEY:'must-not-leak'
 });
 await s.exec('true');expect(state.options[0].network).toEqual({mode:'enabled',allowNet:['postgres']});
 expect(state.commands.at(-1)[2]).toEqual({TANK_DB_URL:'postgresql://readonly@postgres/tank_replay'});await s.destroy?.();
});
it('empty replay allowlist disables networking before the first guest command',async()=>{
 const s=await sandboxFactory({sessionId:'tank-replay-off',workdir:'/tmp/tank-overlay-off'} as any,{
  TANK_SANDBOX_ALLOWED_NET:'[]',TANK_SANDBOX_REPLAY:'1',TANK_SANDBOX_REPLAY_DB_URL:'postgresql://readonly@postgres/tank_replay'
 });
 await s.exec('true');expect(state.options[0].network).toEqual({mode:'disabled'});await s.destroy?.();
});
it('local verified OCI layout reaches the native SimpleBox loader independently of Docker cache',async()=>{
 const s=await sandboxFactory({sessionId:'tank-local-oci',workdir:'/tmp/tank-oci-test'} as any,{
  SANDBOX_ROOTFS_PATH:'/opt/tank-sandbox-oci',SANDBOX_IMAGE:'diagnostic-tag',TANK_SANDBOX_ALLOWED_NET:'unrestricted',TANK_SANDBOX_REPLAY:'0',TANK_SANDBOX_DB_URL:'postgresql://readonly@postgres/tank'
 });
 await s.exec('true');expect(state.options[0].rootfsPath).toBe('/opt/tank-sandbox-oci');await s.destroy?.();
});
