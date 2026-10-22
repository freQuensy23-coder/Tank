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
function source(archive: Uint8Array) { return {retrieveSkillVersion: vi.fn(async()=>({type:'found',version:{version:'1792364806411000',name:'tank-data',description:''}})),downloadSkillVersion:vi.fn(async()=>({type:'found',file:{content:archive}})),listSkillVersions:vi.fn()}; }
const worker = readFileSync(new URL('../../../packages/control-plane-node/src/lib/node-managed-skill-aliases.py', import.meta.url), 'utf8');
async function materialized(archive: Uint8Array) {
  // Model only the guest filesystem boundary. The real preparer, stable plan and
  // batch encoder run; observed files come from their emitted ZIP bytes.
  const files = new Map<string,Uint8Array>(), staged = new Map<string,string>();
  const binding = {name:'tank-data',skill_id:'skill_inputs',version:'1792364806411000',
    archive_sha256:createHash('sha256').update(archive).digest('hex')};
  const quote = (value:string) => "'" + value.replaceAll("'", "'\\''") + "'";
  const prefix = 'python3 -c ' + quote(worker) + ' ';
  let checked = false, claimed = false;
  const sandbox = {
    setEnvVars:async()=>{},
    writeFileBytes:async(path:string, bytes:Uint8Array)=>{
      expect(staged.has(path)).toBe(true);
      expect(files.has(path)).toBe(false);
      expect(createHash('sha256').update(bytes).digest('hex')).toBe(staged.get(path));
      files.set(path, bytes);return path;
    },
    exec:async(command:string, timeout:number)=>{
      if(command==='mkdir -p /mnt/session/outputs'){
        expect(timeout).toBe(30000);return 'exit=0\n';
      }
      if(!command.startsWith(prefix))throw Error('Unexpected skill worker source');
      const args=command.slice(prefix.length);
      const claim=/^(check|claim) '([A-Za-z0-9+/=]+)'$/u.exec(args);
      if(claim){
        expect(timeout).toBe(30000);
        expect(JSON.parse(Buffer.from(claim[2]!,'base64').toString())).toEqual([binding]);
        if(claim[1]==='check')checked=true;
        else {expect(checked).toBe(true);claimed=true;}
        return 'exit=0\nTANK_STABLE_SKILL_BINDINGS_OK\n';
      }
      const batch=/^(stage|materialize|discard) '(\/tmp\/\.openma-skills-[0-9a-f]{32}\/([0-9a-f]{64})\.zip)' '([0-9a-f]{64})'$/u.exec(args);
      if(!batch)throw Error('Unexpected skill worker arguments');
      const [,operation,path,pathDigest,digest]=batch;
      expect(pathDigest).toBe(digest);expect(claimed).toBe(true);
      expect(timeout).toBe(operation==='materialize'?120000:30000);
      if(operation==='stage'){
        expect(staged.has(path!)).toBe(false);expect(files.has(path!)).toBe(false);
        staged.set(path!,digest!);return 'exit=0\nTANK_STABLE_SKILL_STAGE_OK\n';
      }
      expect(staged.get(path!)).toBe(digest);
      if(operation==='discard'){
        files.delete(path!);staged.delete(path!);return 'exit=0\nTANK_STABLE_SKILL_DISCARD_OK\n';
      }
      const encoded=files.get(path!);expect(encoded).toBeDefined();
      expect(createHash('sha256').update(encoded!).digest('hex')).toBe(digest);
      const entries=unzipSync(encoded!);
      expect(JSON.parse(Buffer.from(entries['__tank_skill_batch__.json']!).toString())).toEqual({
        version:1,scopes:[{binding,canonical_root:base}],
      });
      const original=unzipSync(archive), expected=new Map<string,Uint8Array>();
      const directory=Object.hasOwn(original,'tank-data/SKILL.md')?'tank-data/':'';
      for(const [name,bytes] of Object.entries(original))if(!name.endsWith('/')){
        expected.set(base+name,bytes);
        expected.set('/home/user/.skills/tank-data/'+name.slice(directory.length),bytes);
      }
      expect(Object.keys(entries).sort()).toEqual([
        '__tank_skill_batch__.json',...[...expected.keys()].map(name=>name.slice(1)),
      ].sort());
      for(const [name,bytes] of expected){
        expect(entries[name.slice(1)]).toEqual(bytes);
        files.set(name,entries[name.slice(1)]!);
      }
      files.delete(path!);staged.delete(path!);return 'exit=0\nTANK_STABLE_SKILL_FILES_OK\n';
    },
  };
  const preparer = new NodeManagedSessionInputPreparer({sharedOutputs:true,files:{} as any,skillVersions:source(archive) as any,repositoryCredentials:{} as any,memorySnapshots:{} as any});
  await preparer.prepare({workspaceId:'default',session:session(),sandbox:sandbox as any});
  expect(checked && claimed).toBe(true);expect(staged.size).toBe(0);
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
    const body='---\nname: tank-data\ndescription: Root skill\n---\nroot skill';
    const archive=zipSync({'SKILL.md':bytes(body),'references/guide.md':bytes('guide')});mode.value='progressive';
    const [mount]=await resolveNodeManagedSkillMounts(session(),source(archive) as any);expect(mount.mountRoot).toBe(base);expect((await materialized(archive)).get(base+'SKILL.md')).toEqual(bytes(body));
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
