import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { zipSync, unzipSync } from 'fflate';
import { describe, it, expect, vi } from 'vitest';
import { NodeManagedSessionInputPreparer, resolveNodeManagedSkillMounts } from '../../../packages/control-plane-node/src/lib/node-managed-session-inputs.js';
import { stableSkillPlan } from '../../../packages/control-plane-node/src/lib/node-managed-stable-skills.js';

const registered = new Uint8Array(readFileSync(new URL('./fixtures/tank-registered-skill.zip', import.meta.url)));
const at='2026-10-19T00:00:00Z';
const knownWorker=readFileSync(new URL('../../../packages/control-plane-node/src/lib/node-managed-skill-aliases.py',import.meta.url),'utf8');
function knownClaim(command:string) {
  const quote=(value:string)=>`'${value.replaceAll("'","'\\''")}'`;
  const prefix=`python3 -c ${quote(knownWorker)} `;
  if(!command.startsWith(prefix))throw new Error('Unexpected claim worker source');
  const parsed=/^(check|claim) '([A-Za-z0-9+/=]+)'$/u.exec(command.slice(prefix.length));
  if(!parsed)throw new Error('Unexpected claim arguments');
  expect(JSON.parse(Buffer.from(parsed[2]!,'base64').toString())).toEqual([{name:'tank-data',skill_id:'skill-data',version:'1792364806411000',archive_sha256:'c6d0d6c0eb69b355069ca6c954ff5f477e1003cf47fa73257fb5cfc7765cbe20'}]);
}
function session(skills=[{type:'custom',skillId:'skill-data',version:'1792364806411000'}]) {return {id:'alias-test',agent:{id:'agent',name:'Agent',skills,model:{id:'model'},tools:[],mcpServers:[],multiagent:null,system:null,version:1},metadata:{},resources:[],createdAt:at,updatedAt:at} as any;}
function source(archive=registered,name='tank-data') {return {retrieveSkillVersion:vi.fn(async({version})=>({type:'found',version:{version,name,description:'Data guide'}})),listSkillVersions:vi.fn(async()=>({type:'page',page:{versions:[{version:'1792364806411000',name,description:'Data guide'}]}})),downloadSkillVersion:vi.fn(async()=>({type:'found',file:{content:archive}}))};}
function setup(s=session(),versions=source()) {
  const files=new Map<string,Uint8Array>(),commands:string[]=[];
  const sandbox={setEnvVars:async()=>{},writeFileBytes:async(p:string,b:Uint8Array)=>{files.set(p,b);return p;},exec:async(c:string,t:number)=>{commands.push(c);if(c.startsWith('python3 -c')){knownClaim(c);expect(t).toBe(30000);return 'exit=0\nTANK_STABLE_SKILL_BINDINGS_OK\n';}return 'exit=0\n';}};
  const preparer=new NodeManagedSessionInputPreparer({sharedOutputs:true,files:{} as any,skillVersions:versions as any,repositoryCredentials:{} as any,memorySnapshots:{} as any});
  return {files,commands,run:()=>preparer.prepare({workspaceId:'default',session:s,sandbox:sandbox as any}),sandbox};
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
    expect(test.commands.filter(c=>c.startsWith('python3 -c'))).toHaveLength(2);
  });
  it('uses concrete immutable version for latest binding without changing latest descriptor identity',async()=>{
    const s=session([{type:'custom',skillId:'skill-data',version:'latest'}]);const test=setup(s);await test.run();
    const cmd=test.commands.find(c=>c.includes(' claim '))!;const payload=cmd.match(/'([A-Za-z0-9+/=]+)'$/u)![1]!;
    expect(JSON.parse(Buffer.from(payload,'base64').toString())[0]).toMatchObject({name:'tank-data',skill_id:'skill-data',version:'1792364806411000'});
    expect(test.files.has('/workspace/.openma/skills/skill-data/latest/tank-data/SKILL.md')).toBe(true);
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
