import gzip
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
import httpx
import pytest
from tank.sources import SourceService, parsers

RSS=b'''<rss version="2.0"><channel><title>Official</title><item><guid>release-42</guid><title>New protocol</title><link>https://example.org/a</link><pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate><description>&lt;p&gt;Protocol deployment &amp;amp; vote&lt;/p&gt;</description></item></channel></rss>'''

class Repo:
    def __init__(self): self.rows={}; self.jobs=[]
    async def get(self,c,k): return self.rows.get((c,k))
    async def put(self,c,k,v): self.rows[c,k]=dict(v); return v
    async def list(self,*a,**k): return []
    async def enqueue(self,*a,**k): self.jobs.append((a,k))
    @asynccontextmanager
    async def lock(self,key): yield
    @asynccontextmanager
    async def transaction(self): yield

def settings(tmp_path,sources):
    return SimpleNamespace(data_dir=tmp_path,user_agent='Tank test',config_version='test',code_version='test',x_list_id=None,load_config=lambda name:{'sources':sources} if name=='sources' else {'x_accounts':[],'tokens':[],'entities':[]})

def test_rss_atom():
    row=parsers.feeds(RSS,'S30',{'url':'https://example.org/rss'})[0]
    assert row['external_id']=='release-42'
    assert row['published_at']=='2026-10-16T12:00:00Z'
    assert row['text']=='Protocol deployment & vote'
    atom=b'<feed xmlns="http://www.w3.org/2005/Atom"><title>Commits</title><entry><id>tag:github.com,2008:Grit::Commit/abc</id><title>Fix swap</title><updated>2026-10-16T12:00:00Z</updated><link href="https://github.com/org/repo/commit/abc"/><content type="html">&lt;p&gt;Fix routing&lt;/p&gt;</content></entry></feed>'
    row=parsers.feeds(atom,'S25',{'url':'https://github.com/org/repo/commits/main.atom'})[0]
    assert row['subtype']=='commits' and row['text']=='Fix routing'

def test_html_patterns_and_article():
    feed={'url':'https://example.org/news','article_pattern':r'^/news/[a-z]+$'}
    rows=parsers.article_links('<a href="/news/test">Test</a><a href="https://evil.org/news/test">Bad</a>',feed)
    assert len(rows)==1 and rows[0]['url']=='https://example.org/news/test'
    with pytest.raises(ValueError,match='layout'): parsers.article_links('<a href="/">Home</a>',feed)
    row=parsers.article('<nav>Ignore</nav><article><h1>Title</h1><time datetime="2026-10-16T12:00:00Z"></time><p>Evidence</p><script>bad()</script></article>')
    assert row['text']=='Title Evidence'

def test_json_contracts():
    row=parsers.json_items({'result':{'list':[{'url':'https://bybit.com/a','title':'Listing','description':'<p>ARB</p>','type':{'key':'new_crypto'},'dateTimestamp':1679045608000}]}},'S11')[0]
    assert row['text']=='ARB' and row['announcement_type']['key']=='new_crypto'
    row=parsers.json_items({'data':[{'url':'https://okx.com/a','title':'New','pTime':'1679045608000'}]},'S10')[0]
    assert row['external_id'].endswith('|1679045608000')
    row=parsers.json_items({'data':{'catalogs':[{'articles':[{'code':'abc','title':'Release','releaseDate':1679045608000}]}]}},'S09')[0]
    assert row['external_id']=='abc'
    row=parsers.json_items({'data':{'proposals':[{'id':'0xabc','title':'Vote','body':'Proposal','state':'closed','start':1679045608,'end':1679046608,'scores':[1,2],'scores_total':3,'space':{'id':'cow.eth'}}]}},'S24')[0]
    assert row['external_id']=='0xabc|closed' and row['scores_total']==3

@pytest.mark.parametrize('venue,payload,symbol',[
 ('binance',{'symbols':[{'symbol':'ABCUSDT','baseAsset':'ABC','quoteAsset':'USDT','status':'TRADING'}]},'ABCUSDT'),
 ('coinbase',[{'id':'ABC-USD','base_currency':'ABC','quote_currency':'USD','status':'online'}],'ABC-USD'),
 ('kraken',{'error':[],'result':{'ABCUSD':{'base':'ABC','quote':'USD','status':'online'}}},'ABCUSD'),
 ('okx',{'code':'0','data':[{'instId':'ABC-USDT-SWAP','state':'live','ctType':'linear'}]},'ABC-USDT-SWAP'),
 ('bybit',{'retCode':0,'result':{'list':[{'symbol':'ABCUSDT','baseCoin':'ABC','quoteCoin':'USDT','status':'Trading'}]}},'ABCUSDT'),
 ('hyperliquid',{'universe':[{'name':'ABC','isDelisted':False}]},'ABC'),
])
def test_instrument_adapters(venue,payload,symbol):
    assert parsers.instruments(payload,{'venue':venue})[symbol]['status']

@pytest.mark.asyncio
async def test_raw_first_dedup_and_conditional_get(tmp_path):
    calls=[]
    def handle(request):
        calls.append(request)
        return httpx.Response(304) if request.headers.get('If-None-Match') else httpx.Response(200,content=RSS,headers={'ETag':'"v1"'})
    http=httpx.AsyncClient(transport=httpx.MockTransport(handle))
    repo=Repo(); svc=SourceService(repo,settings(tmp_path,[{'source_id':'S30','feeds':[{'url':'https://example.org/rss','kind':'rss'}]}]),http=http)
    assert (await svc.poll_once('S30'))['items']==1
    assert (await svc.poll_once('S30'))['items']==0
    record=next(v for (c,k),v in repo.rows.items() if c=='ingest.source_items')
    assert gzip.decompress(__import__('pathlib').Path(record['raw_ref']).read_bytes())==RSS
    assert repo.jobs[0][0][0]=='FILTER_ITEM'
    await svc.ingest(svc.sources['S30'],parsers.feeds(RSS,'S30',{'url':'https://example.org/rss'})[0],record['raw_ref'],record['raw_sha256'])
    assert len(repo.jobs)==1
    assert (await repo.get('ingest.source_items',record['item_id']))['observation_count']==2
    await http.aclose()

def test_all_34_source_configurations():
    from tank.settings import Settings
    cfg=Settings().load_config('sources')['sources']
    assert {s['source_id'] for s in cfg}=={f'S{i:02d}' for i in range(1,35)}
    for source in cfg:
        assert source['feeds']
        for feed in source['feeds']:
            assert feed['kind'] in {'rss','atom','html','json','graphql','instruments','x','rendered_html','rendered'}
            assert feed['url'].startswith('https://')
            if feed['kind'] in {'html','rendered','rendered_html'}:
                assert feed['article_pattern']
                __import__('re').compile(feed['article_pattern'])
            if feed['kind']=='instruments': assert feed['venue'] in {'binance','coinbase','kraken','okx','bybit','hyperliquid'}
            if feed.get('method')=='POST': assert feed['body']


def test_binance_rich_document():
    payload=json.dumps({'type':'root','children':[{'type':'paragraph','children':[{'text':'We list ABC'},{'text':'on Monday.'}]}]})
    assert parsers.binance_body(payload)=='We list ABC on Monday.'
