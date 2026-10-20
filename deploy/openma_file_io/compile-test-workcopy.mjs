import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {stripTypeScriptTypes} from 'node:module';
import {join} from 'node:path';
const root=process.argv[2];
await mkdir(root,{recursive:true});
await writeFile(join(root,'package.json'),'{"type":"module"}\n');
for(const file of ['boxlite-provider','guest-file-io','guest-file-worker']){
 const source=await readFile(join(root,file+'.ts'),'utf8');
 await writeFile(join(root,file+'.js'),stripTypeScriptTypes(source,{mode:'strip',sourceUrl:file+'.ts'}));
}
