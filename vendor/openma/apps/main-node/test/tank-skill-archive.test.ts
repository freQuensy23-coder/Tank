import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { zipSync, unzipSync } from 'fflate';
import { describe, it, expect, vi } from 'vitest';
const mode = vi.hoisted(() => ({ value: 'progressive' }));
vi.mock('@open-managed-agents/agent/harness/assembly/skill-config', async importOriginal => {
  const original = await importOriginal<object>();
  return {...original, resolveSkillAssemblyConfig: () => ({mode: mode.value, listBudgetFraction: .02, maxDescriptionChars: 1024})};
});
import { skillArchiveManifest } from '../../../packages/control-plane-node/src/lib/node-managed-skill-manifest.js';
import { resolveNodeManagedSkillMounts, NodeManagedSessionInputPreparer } from '../../../packages/control-plane-node/src/lib/node-managed-session-inputs.js';
const bytes = (s: string) => new TextEncoder().encode(s);
const base = '/workspace/.openma/skills/skill_inputs/1792364806411000/';
function session() { return { id:'skill-path-qualification', agent:{id:'agent',name:'Verifier',description:null,model:{id:'model'},mcpServers:[],multiagent:null,skills:[{type:'custom',skillId:'skill_inputs',version:'1792364806411000'}],system:null,tools:[],version:1}, resources:[],metadata:{},createdAt:'2026-10-19T00:00:00Z',updatedAt:'2026-10-19T00:00:00Z' } as any; }
function source(archive: Uint8Array) { return {retrieveSkillVersion: vi.fn(async()=>({type:'found',version:{version:'1792364806411000',name:'A different metadata name',description:''}})),downloadSkillVersion:vi.fn(async()=>({type:'found',file:{content:archive}})),listSkillVersions:vi.fn()}; }
async function materialized(archive: Uint8Array) {
  const files = new Map<string,Uint8Array>();
  const sandbox = {exec:async()=>'', setEnvVars:async()=>{},writeFileBytes:async(path:string,b:Uint8Array)=>{files.set(path,b);return path;}} as any;
  const preparer = new NodeManagedSessionInputPreparer({sharedOutputs:true,files:{} as any,skillVersions:source(archive) as any,repositoryCredentials:{} as any,memorySnapshots:{} as any});
  await preparer.prepare({workspaceId:'default',session:session(),sandbox});
  return files;
}
describe('archive manifest and canonical skill exposure',()=>{
  const registered = new Uint8Array(readFileSync(new URL('./fixtures/tank-registered-skill.zip', import.meta.url)));
  it('uses the exact publicly registered archive bytes, not a reconstructed ZIP',()=>{
    expect(createHash('sha256').update(registered).digest('hex')).toBe('c6d0d6c0eb69b355069ca6c954ff5f477e1003cf47fa73257fb5cfc7765cbe20');
    const m=skillArchiveManifest(unzipSync(registered));expect(m.path).toBe('tank-data/SKILL.md');expect(m.directory).toBe('tank-data/');
    expect(createHash('sha256').update(bytes(m.body)).digest('hex')).toBe('2b2bc5deea8b89aed35b26a77dbeb1baddf2af792f920d09b8bff5bbff51a2b0');
  });
  for(const selected of ['progressive','budgeted','inline','tool'])it(`${selected}: advertised SKILL.md and relative guide match actual materialized registered bytes`,async()=>{
    mode.value=selected;const src=source(registered);const [mount]=await resolveNodeManagedSkillMounts(session(),src as any);
    expect(src.downloadSkillVersion).toHaveBeenCalledOnce();expect(mount.mountRoot).toBe(base+'tank-data/');
    const files=await materialized(registered),m=skillArchiveManifest(unzipSync(registered));
    expect(files.get(mount.mountRoot+'SKILL.md')).toEqual(bytes(m.body));
    expect(files.get(mount.mountRoot+'references/DATA_GUIDE.md')).toEqual(unzipSync(registered)['tank-data/references/DATA_GUIDE.md']);
    expect(files.has(base+'SKILL.md')).toBe(false);expect(mount.body).toBe(selected==='progressive'?undefined:m.body);
  });
  it('supports a safe unprefixed root archive without adding a metadata-name directory',async()=>{
    const archive=zipSync({'SKILL.md':bytes('root skill'),'references/guide.md':bytes('guide')});mode.value='progressive';
    const [mount]=await resolveNodeManagedSkillMounts(session(),source(archive) as any);expect(mount.mountRoot).toBe(base);expect((await materialized(archive)).get(base+'SKILL.md')).toEqual(bytes('root skill'));
  });
  it('supports requested latest mount identity while resolving a concrete archive version',async()=>{
    const s=session();s.agent.skills[0].version='latest';const src=source(registered);src.listSkillVersions=vi.fn(async()=>({type:'page',page:{versions:[{version:'1792364806411000',name:'tank-data',description:''}]}})) as any;
    const [mount]=await resolveNodeManagedSkillMounts(s,src as any);expect(mount.mountRoot).toBe('/workspace/.openma/skills/skill_inputs/latest/tank-data/');expect(src.downloadSkillVersion).toHaveBeenCalledWith({skillId:'skill_inputs',version:'1792364806411000'});
  });
  for(const raw of ['../SKILL.md','/SKILL.md','nested/../../SKILL.md','C:/SKILL.md','nul\0/SKILL.md'])it(`refuses unsafe archive path ${JSON.stringify(raw)}`,()=>{expect(()=>skillArchiveManifest({[raw]:bytes('unsafe')})).toThrow('Unsafe skill archive path');});
  it('refuses missing and ambiguous manifests',()=>{
    expect(()=>skillArchiveManifest({'README.md':bytes('missing')})).toThrow('exactly one');
    expect(()=>skillArchiveManifest({'a/SKILL.md':bytes('a'),'b/SKILL.md':bytes('b')})).toThrow('exactly one');
  });
  it('refuses normalized aliases and invalid UTF-8 manifest bytes',()=>{
    expect(()=>skillArchiveManifest({'a/../SKILL.md':bytes('one'),'SKILL.md':bytes('two')})).toThrow('Duplicate normalized');
    expect(()=>skillArchiveManifest({'SKILL.md':new Uint8Array([255])})).toThrow();
  });
  it('resolver propagates malformed/missing ZIP and missing archive failures before descriptor exposure',async()=>{
    for(const archive of [new Uint8Array([1,2,3]),zipSync({'README.md':bytes('missing')})])await expect(resolveNodeManagedSkillMounts(session(),source(archive) as any)).rejects.toThrow();
    const src=source(registered);src.downloadSkillVersion=vi.fn(async()=>({type:'not_found'})) as any;await expect(resolveNodeManagedSkillMounts(session(),src as any)).rejects.toThrow('archive was not found');
  });
});
