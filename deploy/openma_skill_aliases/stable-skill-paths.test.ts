import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { zipSync, unzipSync } from 'fflate';
import { describe, it, expect, vi } from 'vitest';
import { NodeManagedSessionInputPreparer, resolveNodeManagedSkillMounts } from '../../../packages/control-plane-node/src/lib/node-managed-session-inputs.js';
import { stableSkillPlan } from '../../../packages/control-plane-node/src/lib/node-managed-stable-skills.js';

const registered = new Uint8Array(readFileSync(new URL('./fixtures/tank-registered-skill.zip', import.meta.url)));
const at='2026-10-19T00:00:00Z';
const knownWorker=readFileSync(new URL('../../../packages/control-plane-node/src/lib/node-managed-skill-aliases.py',import.meta.url),'utf8');
const binding = {name:'tank-data',skill_id:'skill-data',version:'1792364806411000',archive_sha256:'c6d0d6c0eb69b355069ca6c954ff5f477e1003cf47fa73257fb5cfc7765cbe20'};
type WorkerRequest =
  | {operation:'check'|'claim';bindings:typeof binding[]}
  | {operation:'stage'|'materialize'|'discard';path:string;digest:string};

function knownWorkerRequest(command:string):WorkerRequest {
  const quote=(value:string)=>"'" + value.replaceAll("'","'\\''") + "'";
  const prefix='python3 -c '+quote(knownWorker)+' ';
  if(!command.startsWith(prefix))throw new Error('Unexpected skill worker source');
  const args=command.slice(prefix.length);
  const claim=/^(check|claim) '([A-Za-z0-9+/=]+)'$/u.exec(args);
  if(claim){
    const bindings=JSON.parse(Buffer.from(claim[2]!,'base64').toString());
    expect(bindings).toEqual([binding]);
    return {operation:claim[1] as 'check'|'claim',bindings};
  }
  const batch=/^(stage|materialize|discard) '(\/tmp\/\.openma-skills-[0-9a-f]{32}\/([0-9a-f]{64})\.zip)' '([0-9a-f]{64})'$/u.exec(args);
  if(!batch)throw new Error('Unexpected skill worker arguments');
  expect(batch[3]).toBe(batch[4]);
  return {operation:batch[1] as 'stage'|'materialize'|'discard',path:batch[2]!,digest:batch[4]!};
}
function session(skills=[{type:'custom',skillId:'skill-data',version:'1792364806411000'}]) {return {id:'alias-test',agent:{id:'agent',name:'Agent',skills,model:{id:'model'},tools:[],mcpServers:[],multiagent:null,system:null,version:1},metadata:{},resources:[],createdAt:at,updatedAt:at} as any;}
function source(archive=registered,name='tank-data') {return {retrieveSkillVersion:vi.fn(async({version})=>({type:'found',version:{version,name,description:'Data guide'}})),listSkillVersions:vi.fn(async()=>({type:'page',page:{versions:[{version:'1792364806411000',name,description:'Data guide'}]}})),downloadSkillVersion:vi.fn(async()=>({type:'found',file:{content:archive}}))};}
function setup(s=session(),versions=source()) {
  const files=new Map<string,Uint8Array>(),commands:string[]=[],writes:string[]=[],workerRequests:WorkerRequest[]=[];
  const staged=new Map<string,string>();
  let checked=false,receipt:typeof binding|undefined;
  const canonicalRoot='/workspace/.openma/skills/skill-data/'+encodeURIComponent(s.agent.skills[0]!.version)+'/';
  const sandbox={
    setEnvVars:async()=>{},
    writeFileBytes:async(p:string,b:Uint8Array)=>{
      expect(staged.has(p)).toBe(true);
      expect(files.has(p)).toBe(false);
      expect(createHash('sha256').update(b).digest('hex')).toBe(staged.get(p));
      writes.push(p);files.set(p,b);return p;
    },
    exec:async(c:string,t:number)=>{
      commands.push(c);
      if(c==='mkdir -p /mnt/session/outputs'){expect(t).toBe(30000);return 'exit=0\n';}
      const request=knownWorkerRequest(c);
      workerRequests.push(request);
      expect(t).toBe(request.operation==='materialize'?120000:30000);
      if('bindings' in request){
        if(receipt)expect(request.bindings).toEqual([receipt]);
        if(request.operation==='check')checked=true;
        else {expect(checked).toBe(true);receipt={...request.bindings[0]!};}
        return 'exit=0\nTANK_STABLE_SKILL_BINDINGS_OK\n';
      }
      expect(receipt).toEqual(binding);
      if(request.operation==='stage'){
        expect(staged.has(request.path)).toBe(false);
        expect(files.has(request.path)).toBe(false);
        staged.set(request.path,request.digest);
        return 'exit=0\nTANK_STABLE_SKILL_STAGE_OK\n';
      }
      expect(staged.get(request.path)).toBe(request.digest);
      if(request.operation==='discard'){
        files.delete(request.path);staged.delete(request.path);
        return 'exit=0\nTANK_STABLE_SKILL_DISCARD_OK\n';
      }
      const archive=files.get(request.path);
      expect(archive).toBeDefined();
      expect(createHash('sha256').update(archive!).digest('hex')).toBe(request.digest);
      const entries=unzipSync(archive!);
      expect(JSON.parse(Buffer.from(entries['__tank_skill_batch__.json']!).toString())).toEqual({
        version:1,scopes:[{binding,canonical_root:canonicalRoot}],
      });
      const expectedFiles=new Map<string,Uint8Array>();
      for(const [name,bytes] of Object.entries(unzipSync(registered)))if(!name.endsWith('/')){
        expectedFiles.set(canonicalRoot+name,bytes);
        expectedFiles.set('/home/user/.skills/'+name,bytes);
      }
      expect(Object.keys(entries).sort()).toEqual([
        '__tank_skill_batch__.json',...[...expectedFiles.keys()].map(p=>p.slice(1)),
      ].sort());
      for(const [target,bytes] of expectedFiles){
        expect(entries[target.slice(1)]).toEqual(bytes);
        files.set(target,entries[target.slice(1)]!);
      }
      files.delete(request.path);staged.delete(request.path);
      return 'exit=0\nTANK_STABLE_SKILL_FILES_OK\n';
    },
  };
  const preparer=new NodeManagedSessionInputPreparer({sharedOutputs:true,files:{} as any,skillVersions:versions as any,repositoryCredentials:{} as any,memorySnapshots:{} as any});
  return {files,commands,writes,workerRequests,staged,run:()=>preparer.prepare({workspaceId:'default',session:s,sandbox:sandbox as any}),sandbox};
}

describe('stable TDD skill paths using actual resolver/materializer',()=>{
  it('preserves exact registered bytes at both versioned descriptors and required stable paths',async()=>{
    const test=setup();await test.run();const entries=unzipSync(registered);
    expect(createHash('sha256').update(registered).digest('hex')).toBe('c6d0d6c0eb69b355069ca6c954ff5f477e1003cf47fa73257fb5cfc7765cbe20');
    for(const [p,bytes] of Object.entries(entries))if(!p.endsWith('/')){
      expect(test.files.get('/workspace/.openma/skills/skill-data/1792364806411000/'+p)).toEqual(bytes);
      expect(test.files.get('/home/user/.skills/'+p)).toEqual(bytes);
    }
    expect(test.files.has('/home/user/.skills/tank-data/SKILL.md')).toBe(true);
    expect(test.files.has('/home/user/.skills/tank-data/references/DATA_GUIDE.md')).toBe(true);
    const [mount]=await resolveNodeManagedSkillMounts(session(),source() as any);
    expect(mount.mountRoot).toBe('/workspace/.openma/skills/skill-data/1792364806411000/tank-data/');
    expect(test.workerRequests.map(r=>r.operation)).toEqual(['check','claim','stage','materialize']);
    expect(test.writes).toHaveLength(1);
    expect(test.staged.size).toBe(0);
  });
  it('uses concrete immutable version for latest binding without changing latest descriptor identity',async()=>{
    const s=session([{type:'custom',skillId:'skill-data',version:'latest'}]);const test=setup(s);await test.run();
    const claim=test.workerRequests.find(r=>r.operation==='claim')!;
    expect('bindings' in claim && claim.bindings[0]).toMatchObject({name:'tank-data',skill_id:'skill-data',version:'1792364806411000'});
    expect(test.files.has('/workspace/.openma/skills/skill-data/latest/tank-data/SKILL.md')).toBe(true);
  });
  it('retries the same immutable binding without changing materialized paths or bytes',async()=>{
    const test=setup();await test.run();const prior=new Map(test.files);
    await test.run();
    expect(test.files).toEqual(prior);
    expect(test.workerRequests.map(r=>r.operation)).toEqual([
      'check','claim','stage','materialize','check','claim','stage','materialize',
    ]);
    expect(new Set(test.writes).size).toBe(2);
    expect(test.staged.size).toBe(0);
  });
  it('refuses duplicate stable names across IDs or versions before any skill bytes are copied',async()=>{
    for(const second of [{type:'custom',skillId:'other-skill',version:'1792364806411000'},{type:'custom',skillId:'skill-data',version:'different-version'}]){
      const test=setup(session([...session().agent.skills,second]));await expect(test.run()).rejects.toThrow('Duplicate stable skill name');expect(test.files.size).toBe(0);
    }
  });
  it('rejects metadata/manifest mismatch or unsafe metadata before writes',async()=>{
    for(const name of ['other-name','../escape']){const test=setup(session(),source(registered,name));await expect(test.run()).rejects.toThrow();expect(test.files.size).toBe(0);}
  });
  it('supports root manifest bytes without guessing from archive prefix',()=>{
    const body='---\nname: root-skill\ndescription: Root\n---\nKeep unchanged\n';
    const plan=stableSkillPlan({skillId:'id',version:'1',requestedVersion:'1',name:'root-skill',archive:zipSync({'SKILL.md':Buffer.from(body)})});
    expect(plan.files[0]!.aliasPath).toBe('/home/user/.skills/root-skill/SKILL.md');expect(Buffer.from(plan.files[0]!.bytes)).toEqual(Buffer.from(body));
  });
  it('binding claim rejection or unconfirmed worker completion stops before all file writes',async()=>{
    for(const failure of ['exit=1\nBinding mismatch','exit=1\nTANK_STABLE_SKILL_BINDINGS_OK','[exit 1]\nTANK_STABLE_SKILL_BINDINGS_OK','[error:failure]\nTANK_STABLE_SKILL_BINDINGS_OK','exit=0\n']){
      const test=setup();test.sandbox.exec=async(c:string)=>c.startsWith('python3 -c')?failure:'exit=0\n';
      await expect(test.run()).rejects.toThrow();expect(test.files.size).toBe(0);
    }
  });
  it('rejects extra files outside manifest root instead of copying them into the alias',()=>{
    const archive=zipSync({...unzipSync(registered),'other/outside':Buffer.from('unexpected')});
    expect(()=>stableSkillPlan({skillId:'id',version:'1',requestedVersion:'1',name:'tank-data',archive})).toThrow('outside manifest');
  });
});
