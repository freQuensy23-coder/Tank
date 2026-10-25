import { createServer } from 'node:http';
import { createHash } from 'node:crypto';

const observations = [];
const token = process.env.QUALIFICATION_PROVIDER_KEY;
if (!token) throw Error('Fresh isolated provider key required');
const server = createServer(async (request, response) => {
  if (request.url === '/health') { response.end('ok'); return; }
  if (request.headers['x-api-key'] !== token && request.headers.authorization !== 'Bearer ' + token) {
    response.writeHead(401).end(); return;
  }
  if (request.url === '/observations') { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(observations)); return; }
  try {
    let raw = '';
    for await (const chunk of request) { raw += chunk; if (raw.length > 8 * 1024 ** 2) throw Error('Provider request too large'); }
    const body = JSON.parse(raw);
    const messages = body.messages;
    if (!Array.isArray(messages)) throw Error('Expected Anthropic messages');
    const userTexts = messages.filter(message => message.role === 'user').flatMap(message => typeof message.content === 'string' ? [message.content] : message.content.filter(block => block.type === 'text').map(block => block.text));
    const commands = userTexts.flatMap(text => [...text.matchAll(/TANK_QUALIFICATION_COMMAND:([A-Za-z0-9+/=]+)/g)].map(match => Buffer.from(match[1], 'base64').toString()));
    if (!commands.length) throw Error('Unexpected non-qualification model request; refusing external fallback');
    const digest = command => createHash('sha256').update(command).digest('hex');
    const issued = new Set(messages.filter(message => message.role === 'assistant').flatMap(message => Array.isArray(message.content) ? message.content.filter(block => block.type === 'tool_use').map(block => block.id) : []));
    const command = commands.find(command => !issued.has('toolu_' + digest(command).slice(0, 24)));
    const toolResult = command === undefined;
    const key = digest(command ?? commands.at(-1));
    const content = toolResult ? { type: 'text', text: 'QUALIFICATION_COMPLETED' } : { type: 'tool_use', id: 'toolu_' + key.slice(0, 24), name: 'bash', input: { command, timeout: 120000 } };
    observations.push({ commandSha256: key, kind: content.type, upgradedNoticePresent: /runtime.*upgrad|upgrad.*runtime/i.test(JSON.stringify(body.system)) });
    const id = 'msg_' + key.slice(0, 24);
    const message = { id, type: 'message', role: 'assistant', model: body.model, content: [content], stop_reason: toolResult ? 'end_turn' : 'tool_use', stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } };
    if (!body.stream) { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(message)); return; }
    response.setHeader('content-type', 'text/event-stream');
    const event = (type, data) => response.write('event: ' + type + '\ndata: ' + JSON.stringify({ type, ...data }) + '\n\n');
    event('message_start', { message: { ...message, content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } });
    event('content_block_start', { index: 0, content_block: toolResult ? { type: 'text', text: '' } : { ...content, input: {} } });
    event('content_block_delta', { index: 0, delta: toolResult ? { type: 'text_delta', text: content.text } : { type: 'input_json_delta', partial_json: JSON.stringify(content.input) } });
    event('content_block_stop', { index: 0 });
    event('message_delta', { delta: { stop_reason: message.stop_reason, stop_sequence: null }, usage: { output_tokens: 1 } });
    event('message_stop', {});
    response.end();
  } catch (error) {
    response.writeHead(400, { 'content-type': 'application/json' }).end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: error.message } }));
  }
});
server.listen(9000, '0.0.0.0');
