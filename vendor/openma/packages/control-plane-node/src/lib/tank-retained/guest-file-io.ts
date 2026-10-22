import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { open, mkdtemp, rename, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { GUEST_FILE_WORKER } from './guest-file-worker.js';
export const FILE_LIMIT = 512 * 1024 * 1024;
const CHUNK = 64 * 1024, MAX_LINE = 4 * Math.ceil(CHUNK / 3) + 256;
export type FileProcess = {
  stdin: WritableStream<Uint8Array>; stdout: ReadableStream<Uint8Array>; stderr: ReadableStream<Uint8Array>;
  exited: Promise<{code:number|null;signal:string|null}>; kill(signal?:string):Promise<void>;
};
export type FileRunner = <T>(args:string[], exchange:(process:FileProcess)=>Promise<T>)=>Promise<T>;
const failure = () => new Error('guest_file_protocol_invalid');
function pathArgument(path:string) { if(!path || path.includes('\0') || Buffer.byteLength(path)>16384)throw Error('guest_file_path_invalid'); }
async function lines(stream:ReadableStream<Uint8Array>, consume:(line:string)=>Promise<void>) {
  const reader=stream.getReader(), decoder=new TextDecoder('utf-8',{fatal:true}); let pending='';
  try { for(;;) {
    const row=await reader.read(); if(row.done)break;
    const decoded=decoder.decode(row.value,{stream:true});let offset=0;
    for(;;) {const end=decoded.indexOf('\n',offset);const part=decoded.slice(offset,end<0?undefined:end);if(pending.length+part.length>MAX_LINE)throw failure();pending+=part;if(end<0)break;await consume(pending);pending='';offset=end+1;}
  }
  pending+=decoder.decode();if(pending)throw failure();
  } finally {reader.releaseLock();}
}
async function stderrBound(stream:ReadableStream<Uint8Array>) {
  let size=0;const reader=stream.getReader();try{for(;;){const row=await reader.read();if(row.done)return size;size+=row.value.byteLength;if(size>65536)throw failure();}}finally{reader.releaseLock();}
}
function json(line:string):Record<string,unknown> {try{const v=JSON.parse(line);if(!v||Array.isArray(v)||typeof v!=='object')throw failure();return v;}catch{throw failure();}}
function exact(row:Record<string,unknown>, keys:string[]) {if(Object.keys(row).sort().join()!==keys.sort().join())throw failure();}
function header(row:Record<string,unknown>, operation:string) {if(row.protocol!=='tank-file-io'||row.version!==1||row.operation!==operation||typeof row.ok!=='boolean')throw failure();}
function guestError(row:Record<string,unknown>) {
  if(!Number.isInteger(row.errno)||(row.errno as number)<=0||(row.errno as number)>4095)throw failure();
  const names:Record<number,[string,string]>={1:['EPERM','operation not permitted'],2:['ENOENT','no such file or directory'],5:['EIO','input/output error'],13:['EACCES','permission denied'],20:['ENOTDIR','not a directory'],21:['EISDIR','illegal operation on a directory'],22:['EINVAL','invalid argument'],27:['EFBIG','file too large'],28:['ENOSPC','no space left on device'],30:['EROFS','read-only file system'],40:['ELOOP','too many symbolic links']};
  const number=row.errno as number,[code,message]=names[number]??['ERR_GUEST_FILE_IO','guest file operation failed'];
  return Object.assign(new Error(`${code}: ${message} (guest file operation)`),{code,errno:-number,syscall:'guest-file-io'});
}
function sha(value:unknown) {return typeof value==='string'&&/^[0-9a-f]{64}$/.test(value);}
export async function copyGuestFileOut(run:FileRunner, guest:string, host:string) {
  pathArgument(guest);
  const temporary=await mkdtemp(join(dirname(host),'.tank-file-read-'));const target=join(temporary,'content');
  const output=await open(target,'wx',0o600);let count=0;const digest=createHash('sha256');
  try {
    const result=await run(['-c',GUEST_FILE_WORKER,'read',guest],async process=>{
      const progress:{state:'header'|'data'|'done'}={state:'header'};let size=0, error:Error|undefined;
      const writer=process.stdin.getWriter();
      const consume=async(line:string)=>{
        if(progress.state==='done')throw failure();
        if(progress.state==='header') {const row=json(line);header(row,'read');if(!row.ok){exact(row,['protocol','version','operation','ok','errno']);error=guestError(row);progress.state='done';return;}
          exact(row,['protocol','version','operation','ok','size']);if(!Number.isSafeInteger(row.size)||(row.size as number)<0||(row.size as number)>FILE_LIMIT)throw failure();size=row.size as number;progress.state='data';return;}
        if(line.startsWith('D ')) {
          const encoded=line.slice(2);if(!encoded||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded))throw failure();
          const data=Buffer.from(encoded,'base64');if(data.length>CHUNK||data.toString('base64')!==encoded||count+data.length>size||count+data.length>FILE_LIMIT)throw failure();
          let offset=0;while(offset<data.length){const {bytesWritten}=await output.write(data,offset,data.length-offset);if(!bytesWritten)throw failure();offset+=bytesWritten;}
          digest.update(data);count+=data.length;await writer.write(Buffer.from('A\n'));return;
        }
        const row=json(line);progress.state='done';
        if(row.done===false){exact(row,['done','errno']);error=guestError(row);return;}
        exact(row,['done','bytes','sha256']);if(row.done!==true||row.bytes!==count||count!==size||!sha(row.sha256)||row.sha256!==digest.digest('hex'))throw failure();
      };
      const complete=Promise.all([lines(process.stdout,consume),stderrBound(process.stderr),process.exited]);void complete.catch(()=>undefined);
      let completed:Awaited<typeof complete>;
      try{completed=await complete;await writer.close().catch(()=>undefined);}finally{writer.releaseLock();}
      const [,stderr,exit]=completed;if(progress.state!=='done'||stderr!==0)throw failure();
      if(error){if(exit.code!==1)throw failure();return {error};}if(exit.code!==0)throw failure();return {error:undefined};
    });
    if(result.error)throw result.error;
    await output.close();await rename(target,host);
  } finally {await output.close().catch(()=>undefined);await rm(temporary,{recursive:true,force:true});}
}
export async function copyGuestFileIn(run:FileRunner, host:string, guest:string) {
  pathArgument(guest);const input=await open(host,'r');
  try {
    const info=await input.stat();if(!info.isFile()||info.size>FILE_LIMIT)throw Error('guest_file_source_size_or_type_invalid');
    let count=0;const digest=createHash('sha256');
    const result=await run(['-c',GUEST_FILE_WORKER,'write',guest,String(info.size)],async process=>{
      const progress:{state:'header'|'data'|'footer'|'done'}={state:'header'};
      let error:Error|undefined,expectedDigest:string|undefined;
      let readyResolve:()=>void=()=>{};
      const ready=new Promise<void>(resolve=>{readyResolve=resolve;});
      let ack:{count:number;resolve:()=>void}|undefined;
      const consume=async(line:string)=>{
        if(progress.state==='done')throw failure();const row=json(line);
        if(progress.state==='header'){
          header(row,'write');if(!row.ok){exact(row,['protocol','version','operation','ok','errno']);error=guestError(row);progress.state='done';readyResolve();return;}
          exact(row,['protocol','version','operation','ok','size']);if(row.size!==info.size)throw failure();progress.state='data';readyResolve();return;
        }
        if(row.done===false){exact(row,['done','errno']);error=guestError(row);progress.state='done';ack?.resolve();return;}
        if(progress.state==='data'){
          exact(row,['ack']);if(!ack||row.ack!==ack.count)throw failure();const current=ack;ack=undefined;current.resolve();return;
        }
        exact(row,['done','bytes','sha256']);if(row.done!==true||row.bytes!==count||!sha(row.sha256)||row.sha256!==expectedDigest)throw failure();progress.state='done';
      };
      const complete=Promise.all([lines(process.stdout,consume),stderrBound(process.stderr),process.exited]);void complete.catch(()=>undefined);
      const writer=process.stdin.getWriter();
      try {
        await Promise.race([ready,complete.then(()=>{if(!error)throw failure();})]);
        const buffer=Buffer.alloc(CHUNK);
        while(!error){
          const {bytesRead}=await input.read(buffer,0,CHUNK,null);if(!bytesRead)break;
          if(count+bytesRead>info.size)throw Error('guest_file_source_changed');
          const chunk=buffer.subarray(0,bytesRead);digest.update(chunk);count+=bytesRead;
          let resolveAck:()=>void=()=>{};const acknowledged=new Promise<void>(resolve=>{resolveAck=resolve;});
          ack={count,resolve:resolveAck};
          await Promise.race([Promise.all([writer.write(chunk),acknowledged]),complete.then(()=>{if(!error)throw failure();})]);
        }
        if(!error){if(count!==info.size)throw Error('guest_file_source_changed');expectedDigest=digest.digest('hex');progress.state='footer';}
        await writer.close().catch(cause=>{if(!error)throw cause;});
      }finally{writer.releaseLock();}
      const [,stderr,exit]=await complete;if(progress.state!=='done'||stderr!==0)throw failure();
      if(error){if(exit.code!==1)throw failure();return {error};}if(exit.code!==0)throw failure();return {error:undefined};
    });
    if(result.error)throw result.error;
  } finally {await input.close();}
}
