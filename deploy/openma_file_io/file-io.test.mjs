import {test} from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {Readable,Writable} from 'node:stream';
import {mkdtemp,writeFile,readFile,rm,chmod,open,stat,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {pathToFileURL} from 'node:url';
const compiled=process.env.TANK_FILE_IO_TEST_WORKCOPY;
assert.ok(compiled,'explicit isolated compiled workcopy required');
const {RetainedBoxOperations}=await import(pathToFileURL(join(compiled,'boxlite-provider.js')));
const {FILE_LIMIT,copyGuestFileOut,copyGuestFileIn}=await import(pathToFileURL(join(compiled,'guest-file-io.js')));
const python=process.env.TANK_FILE_IO_TEST_PYTHON;
assert.ok(python,'explicit Python3.12 test interpreter required');
function fixture(){
 const children=[];let kills=0;
 const runtime={async spawnDuplexProcess(spec){
   assert.equal(spec.command,'python3');assert.equal(spec.env,undefined);assert.equal(spec.cwd,'/workspace');
   const child=spawn(python,spec.args,{stdio:['pipe','pipe','pipe'],env:{PATH:'/usr/bin:/bin'}});children.push(child);
   const exited=new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});
   return {stdin:Writable.toWeb(child.stdin),stdout:Readable.toWeb(child.stdout),stderr:Readable.toWeb(child.stderr),exited,async kill(signal){kills++;child.kill(signal);}};
 },async readFileBytes(){assert.fail('native copyOut must not be used');},async writeFileBytes(){assert.fail('native copyIn must not be used');}};
 const box=new RetainedBoxOperations(async operation=>operation(runtime));
 return {box,runtime,children,get kills(){return kills;},async close(){for(const child of children){if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');}await Promise.all(children.map(child=>child.exitCode!==null||child.signalCode!==null?undefined:new Promise(resolve=>child.once('exit',resolve))));}};
}
async function owned(fn){const root=await mkdtemp(join(tmpdir(),'tank-file-api-'));const f=fixture();try{await fn(root,f);}finally{await f.close();await rm(root,{recursive:true,force:true});}}
test('real Python guest file worker transfers binary with NUL, invalid UTF8, quotes, spaces and shell metacharacters',async()=>owned(async(root,f)=>{
 const bytes=Buffer.from([0,255,254,1,10,13,0,128]);const src=join(root,'src'),guest=join(root,"guest dir/';$(do-not-execute) binary"),dst=join(root,'dst');await writeFile(src,bytes);
 await f.box.copyIn(src,guest);await f.box.copyOut(guest,dst);assert.deepEqual(await readFile(dst),bytes);assert.deepEqual(await readFile(guest),bytes);assert.equal(f.kills,0);
}));
test('multi-chunk transfer is streamed without losing bytes and text API bytes remain UTF8 intact',async()=>owned(async(root,f)=>{
 const bytes=Buffer.alloc(2*1024*1024+7);for(let n=0;n<bytes.length;n++)bytes[n]=n%251;const src=join(root,'src'),dst=join(root,'dst'),guest=join(root,'guest');await writeFile(src,bytes);await f.box.copyIn(src,guest);await f.box.copyOut(guest,dst);assert.deepEqual(await readFile(dst),bytes);
 const text=Buffer.from('Unicode π 😀 and newlines\n');await writeFile(src,text);await f.box.copyIn(src,guest);await f.box.copyOut(guest,dst);assert.deepEqual(await readFile(dst),text);
}));
test('empty regular files succeed in both directions',async()=>owned(async(root,f)=>{const src=join(root,'src'),dst=join(root,'dst'),guest=join(root,'guest');await writeFile(src,'');await f.box.copyIn(src,guest);await f.box.copyOut(guest,dst);assert.equal((await stat(dst)).size,0);}));
test('missing path and directory errors cannot replace host destination and do not poison a confirmed-exited runtime',async()=>owned(async(root,f)=>{
 const dst=join(root,'dst');await writeFile(dst,'retain');await assert.rejects(f.box.copyOut(join(root,'missing'),dst),error=>error.code==='ENOENT'&&error.errno===-2&&/no such file/.test(error.message)&&!error.message.includes(root));await assert.rejects(f.box.copyOut(root,dst),/EISDIR:/);assert.equal(await readFile(dst,'utf8'),'retain');await writeFile(join(root,'ok'),'ok');await f.box.copyOut(join(root,'ok'),dst);assert.equal(await readFile(dst,'utf8'),'ok');assert.ok(!(await readdir(root)).some(n=>n.startsWith('.tank-file-read-')));
}));
test('kernel read/write permission errors preserve existing bytes and allow later ordinary I/O',async()=>owned(async(root,f)=>{
 assert.notEqual(process.getuid(),0,'run this contract unprivileged');const src=join(root,'src'),guest=join(root,'guest'),dst=join(root,'dst');await writeFile(src,Buffer.alloc(256*1024,1));await writeFile(guest,'unchanged');await chmod(guest,0o444);
 await assert.rejects(f.box.copyIn(src,guest),/EACCES: permission denied/);assert.equal(await readFile(guest,'utf8'),'unchanged');await chmod(guest,0);await assert.rejects(f.box.copyOut(guest,dst),/EACCES: permission denied/);await chmod(guest,0o644);await f.box.copyOut(guest,dst);assert.equal(await readFile(dst,'utf8'),'unchanged');
}));
test('upstream512MiB cap is checked before read/write transfer, including a sparse source',async()=>owned(async(root,f)=>{
 const src=join(root,'huge'),fd=await open(src,'w');await fd.truncate(FILE_LIMIT+1);await fd.close();await assert.rejects(f.box.copyIn(src,join(root,'guest')),/source_size_or_type_invalid/);assert.equal(f.children.length,0);await assert.rejects(f.box.copyOut(src,join(root,'dst')),/EFBIG: file too large/);await assert.rejects(stat(join(root,'dst')),e=>e.code==='ENOENT');
}));
test('NUL/oversized path is refused before process launch',async()=>owned(async(root,f)=>{await assert.rejects(f.box.copyOut('bad\0path',join(root,'dst')),/path_invalid/);await assert.rejects(f.box.copyOut('x'.repeat(16385),join(root,'dst')),/path_invalid/);assert.equal(f.children.length,0);}));
function processWith(stdout,code=0){return {stdin:new WritableStream(),stdout:new Response(stdout).body,stderr:new Response('').body,exited:Promise.resolve({code,signal:null}),async kill(){}};}
test('strict read protocol refuses absent/duplicate headers, oversized lines, wrong digest and nonzero success',async()=>owned(async(root)=>{
 const head=JSON.stringify({protocol:'tank-file-io',version:1,operation:'read',ok:true,size:0})+'\n';const done=JSON.stringify({done:true,bytes:0,sha256:'0'.repeat(64)})+'\n';
 for(const [wire,code] of [['',0],[head+head,0],['x'.repeat(90000)+'\n',0],[head+done,0],[head+JSON.stringify({done:true,bytes:0,sha256:'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})+'\n',3]]){
  const dst=join(root,'dst');await writeFile(dst,'unchanged');await assert.rejects(copyGuestFileOut(async(args,exchange)=>exchange(processWith(wire,code)),'/guest',dst),/protocol_invalid/);assert.equal(await readFile(dst,'utf8'),'unchanged');
 }
}));
test('deferred spawn and stalled pipe still obey original requested exec deadline and refuse overlap',async()=>{
 let release,killed=0;const pending=new Promise(resolve=>{release=resolve;});const runtime={async spawnDuplexProcess(){return pending;}};const box=new RetainedBoxOperations(async fn=>fn(runtime));
 const started=performance.now();await assert.rejects(box.exec('not-run',[],undefined,{timeoutSecs:.01}),/retained_exec_timeout/);assert.ok(performance.now()-started<500);
 release({...processWith(''),async kill(signal){assert.equal(signal,'SIGKILL');killed++;}});await new Promise(r=>setImmediate(r));assert.equal(killed,1);await assert.rejects(box.exec('overlap',[]),/requires_stop/);
});
test('hung write kills only its exact child, poisons the runtime, and permits recovery only on a fresh runtime',{timeout:3000},async()=>owned(async(root,f)=>{
 const unrelated=spawn(python,['-c','import time; time.sleep(20)'],{stdio:'ignore',env:{PATH:'/usr/bin:/bin'}});
 let releaseWrite;const stalled=new Promise(resolve=>{releaseWrite=resolve;});const normal=f.runtime.spawnDuplexProcess;
 f.runtime.spawnDuplexProcess=async spec=>{const process=await normal(spec);return {...process,stdin:new WritableStream({async write(){await stalled;},close(){},abort(){}})};};
 const src=join(root,'src'),guest=join(root,'partial');await writeFile(src,Buffer.alloc(128*1024,1));
 const {copyGuestFileIn}=await import(pathToFileURL(join(compiled,'guest-file-io.js')));
 const started=performance.now();
 try {
  await assert.rejects(copyGuestFileIn((args,exchange)=>f.box.run('python3',args,undefined,{timeoutSecs:.15},exchange),src,guest),/retained_exec_timeout/);
  assert.ok(performance.now()-started<1000);await assert.rejects(f.box.exec('overlap',[]),/requires_stop/);
  await new Promise(resolve=>setTimeout(resolve,30));assert.equal(f.kills,1);assert.ok(f.children.every(child=>child.exitCode!==null||child.signalCode==='SIGKILL'));assert.equal(unrelated.exitCode,null);assert.equal(unrelated.signalCode,null);
  const fresh=fixture();try{await fresh.box.copyIn(src,join(root,'fresh'));await fresh.box.copyOut(join(root,'fresh'),join(root,'readback'));assert.deepEqual(await readFile(join(root,'readback')),await readFile(src));}finally{await fresh.close();}
 }finally{releaseWrite();unrelated.kill('SIGKILL');await new Promise(resolve=>unrelated.once('exit',resolve));}
}));
test('malformed read while process remains live triggers exact transport cleanup and no queued file side effects',async()=>owned(async(root)=>{
 let killed=0,spawned=0;const runtime={async spawnDuplexProcess(){spawned++;return {stdin:new WritableStream(),stdout:new Response('{"bad":"frame"}\n').body,stderr:new Response('').body,exited:new Promise(()=>{}),async kill(signal){assert.equal(signal,'SIGKILL');killed++;}};}};
 const box=new RetainedBoxOperations(async fn=>fn(runtime));await assert.rejects(box.copyOut('/public/file',join(root,'dst')),/protocol_invalid/);assert.equal(killed,1);await writeFile(join(root,'valid-source'),'no side effects');await assert.rejects(box.copyIn(join(root,'valid-source'),'/other'),/requires_stop/);await assert.rejects(box.exec('overlap',[]),/requires_stop/);assert.equal(spawned,1);await assert.rejects(stat(join(root,'dst')),e=>e.code==='ENOENT');
}));
test('write producer cannot enqueue a second64KiB chunk before an actual-write ACK despite an unbounded fake SDK sink',async()=>owned(async(root)=>{
 const input=join(root,'source');const data=Buffer.alloc(2*65536+3,7);await writeFile(input,data);
 let output,exit,received=0,writes=0;const digest=(await import('node:crypto')).createHash('sha256');
 const wire=row=>output.enqueue(Buffer.from(JSON.stringify(row)+'\n'));
 const process={stdin:new WritableStream({write(chunk){assert.ok(chunk.length<=65536);writes++;received+=chunk.length;digest.update(chunk);},close(){wire({done:true,bytes:received,sha256:digest.digest('hex')});output.close();exit({code:0,signal:null});}}),stdout:new ReadableStream({start(controller){output=controller;wire({protocol:'tank-file-io',version:1,operation:'write',ok:true,size:data.length});}}),stderr:new Response('').body,exited:new Promise(resolve=>{exit=resolve;}),async kill(){}};
 const copying=copyGuestFileIn(async(args,exchange)=>exchange(process),input,'/guest/file');
 async function until(count){for(let n=0;n<100&&writes<count;n++)await new Promise(resolve=>setTimeout(resolve,2));assert.equal(writes,count);}
 await until(1);await new Promise(resolve=>setTimeout(resolve,20));assert.equal(writes,1);assert.equal(received,65536);wire({ack:received});
 await until(2);await new Promise(resolve=>setTimeout(resolve,20));assert.equal(writes,2);wire({ack:received});await until(3);wire({ack:received});await copying;assert.equal(received,data.length);
}));
test('actual read worker produces only one data chunk until the consumer ACKs its staged bytes',async()=>owned(async(root)=>{
 const source=join(root,'read-source');await writeFile(source,Buffer.alloc(2*65536,9));
 const {GUEST_FILE_WORKER}=await import(pathToFileURL(join(compiled,'guest-file-worker.js')));
 const child=spawn(python,['-c',GUEST_FILE_WORKER,'read',source],{stdio:['pipe','pipe','pipe'],env:{PATH:'/usr/bin:/bin'}});
 const {createInterface}=await import('node:readline');const rows=[];const input=createInterface({input:child.stdout});input.on('line',line=>rows.push(line));const done=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
 async function until(count){for(let n=0;n<100&&rows.length<count;n++)await new Promise(resolve=>setTimeout(resolve,2));assert.equal(rows.length,count);}
 try{await until(2);await new Promise(resolve=>setTimeout(resolve,30));assert.equal(rows.length,2);assert.equal(child.exitCode,null);child.stdin.write('A\n');await until(3);await new Promise(resolve=>setTimeout(resolve,30));assert.equal(rows.length,3);child.stdin.end('A\n');assert.deepEqual(await done,{code:0,signal:null});await until(4);assert.equal(JSON.parse(rows[3]).done,true);}finally{input.close();if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await done;}}
}));
