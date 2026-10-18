// Live gateway probe: stdin is the ephemeral internal key, never OAuth.
import {readFileSync} from 'node:fs';
const packagePath=process.argv[2], base=process.argv[3];
const key=readFileSync(0,'utf8');
const {stream}=await import(packagePath+'/dist/api/openai-responses.js');
const {normalizeContext}=await import(packagePath+'/dist/utils/transcript.js');
const model={id:'gpt-6.1-sol',name:'Tank live probe',api:'openai-responses',provider:'tank-codex',baseUrl:base,
 reasoning:true,input:['text'],contextWindow:200000,maxTokens:32000,
 cost:{input:0,output:0,cacheRead:0,cacheWrite:0},compat:{supportsMaxOutputTokens:false}};
const context=normalizeContext({systemPrompt:'This is a synthetic transport test. Call verify_probe exactly once with tag synthetic-probe. After its result, answer exactly TANK_LIVE_CODEX_OK. Do nothing else.',
 messages:[{role:'user',content:'Run the synthetic transport verification now.',timestamp:Date.now()}],
 tools:[{name:'verify_probe',description:'Synthetic local roundtrip. No external actions.',parameters:{type:'object',properties:{tag:{type:'string',enum:['synthetic-probe']}},required:['tag'],additionalProperties:false}}]});
let fetches=0;const turns=[];
const options={apiKey:key,reasoningEffort:'low',cacheRetention:'none',
 fetch:async(url,init)=>{if(String(url)!==base+'/responses')throw Error('destination_refused');fetches++;return fetch(url,init);}};
async function turn(extra){
 const events=[];const request=stream(model,context,{...options,...extra});
 for await(const event of request)events.push(event.type);
 const message=await request.result();
 const errorCode=typeof message.errorMessage==='string' ? /(?:Error Code |^)([a-z_]+):/.exec(message.errorMessage)?.[1] : undefined;
 const summary={event_types:[...new Set(events)],stop_reason:message.stopReason,
 usage:{input:message.usage?.input??0,output:message.usage?.output??0},...(errorCode?{error_code:errorCode}:{})};
 turns.push(summary);
 if(events.includes('error'))throw Error('provider_or_stream_error');
 return message;
}
try{
 const first=await turn({toolChoice:'required'});
 const calls=first.content.filter(block=>block.type==='toolCall');
 if(calls.length!==1||calls[0].name!=='verify_probe'||calls[0].arguments?.tag!=='synthetic-probe')throw Error('unexpected_tool_shape');
 context.messages.push(first,{role:'toolResult',toolCallId:calls[0].id,toolName:'verify_probe',content:[{type:'text',text:'synthetic-probe verified'}],isError:false,timestamp:Date.now()});
 const second=await turn({toolChoice:'none'});
 const text=second.content.filter(block=>block.type==='text').map(block=>block.text).join('').trim();
 if(text!=='TANK_LIVE_CODEX_OK')throw Error('unexpected_terminal_text');
 console.log(JSON.stringify({status:'PASS',fetches,tool_calls:1,tool_result_roundtrip:true,expected_text_verified:true,turns}));
}catch(error){
 // Error messages are deliberately reduced to this fixed allowlist.
 const allowed=['destination_refused','provider_or_stream_error','unexpected_tool_shape','unexpected_terminal_text'];
 const code=allowed.includes(error?.message)?error.message:'live_pi_local_failure';
 console.log(JSON.stringify({status:'FAIL',failure_code:code,fetches,turns}));process.exitCode=1;
}
