# Tank V0 — sources and X accounts with links

Built from 02 HLD §3 (sources) and 01 PRD §4.3 (X accounts), 19 Oct 2026. Links point to what Tank reads; for APIs a human page is given next to it. Every link was opened on 19 Oct 2026 (X handles through X's oEmbed; SEC, Quant and Snapshot in a browser, since they refuse scripted requests).

**Who watches what.** The agent does not monitor anything. Tank's `sources` module polls S01–S28 and S30–S34 at the intervals below, `xsensor` reads the X list (S29) every 60 s, and Jev filters each new item and routes it. Only items routed to an event reach the agent's session; inside it the agent can also open pages (`fetch_page`) and search the web (`web_search`).

## Regulators and market structure

| ID | Source | What Tank reads | Interval |
|---|---|---|---|
| S01 | SEC | [press releases RSS](https://www.sec.gov/news/pressreleases.rss) · [speeches & statements RSS](https://www.sec.gov/news/speeches-statements.rss) | 60 s · 120 s |
| S02 | SEC EDGAR | [latest S-1 filings (Atom)](https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=S-1&owner=include&count=40&output=atom) | 120 s |
| S03 | Federal Register | [documents API](https://www.federalregister.gov/api/v1/documents.json) (SEC, CFTC, Fed, OCC, Treasury, FinCEN) | — |
| S04 | CFTC | [press releases RSS](https://www.cftc.gov/RSS/RSSGP/rssgp.xml) · [enforcement RSS](https://www.cftc.gov/RSS/RSSENF/rssenf.xml) | 120 s · 300 s |
| S05 | Federal Reserve | [press RSS](https://www.federalreserve.gov/feeds/press_all.xml) | 120 s |
| S06 | OCC | [news RSS](https://www.occ.gov/rss/occ_news.xml) | 300 s |
| S07 | US Treasury + OFAC | [Treasury press releases](https://home.treasury.gov/news/press-releases) · [OFAC RSS](https://ofac.treasury.gov/rss.xml) | 300 s |
| S08 | DTCC | [press releases](https://www.dtcc.com/press-releases) (the HLD page `dtcc.com/news` now redirects to Insights) | 300 s |

## Exchanges and brokers

| ID | Source | What Tank reads | Interval |
|---|---|---|---|
| S09 | Binance | [announcements](https://www.binance.com/en/support/announcement) (API: new listings, delistings, latest news) | 30 s |
| S10 | OKX | [announcements](https://www.okx.com/help/section/announcements-latest-announcements) (API: [v5/support/announcements](https://www.okx.com/api/v5/support/announcements)) | 60 s |
| S11 | Bybit | [announcements](https://announcements.bybit.com/) (API: [v5/announcements](https://api.bybit.com/v5/announcements/index?locale=en-US)) | 60 s |
| S12 | Kraken | [blog RSS](https://blog.kraken.com/feed) | 120 s |
| S13 | Robinhood | [newsroom](https://robinhood.com/newsroom/) (moved from `newsroom.aboutrobinhood.com`) | 300 s |
| S14 | New markets on 6 venues | [Binance spot](https://api.binance.com/api/v3/exchangeInfo), [Binance perps](https://fapi.binance.com/fapi/v1/exchangeInfo), [Coinbase](https://api.exchange.coinbase.com/products), [Kraken](https://api.kraken.com/0/public/AssetPairs), [OKX spot](https://www.okx.com/api/v5/public/instruments?instType=SPOT), [OKX swaps](https://www.okx.com/api/v5/public/instruments?instType=SWAP), [Bybit spot](https://api.bybit.com/v5/market/instruments-info?category=spot), [Bybit perps](https://api.bybit.com/v5/market/instruments-info?category=linear) | — |

## Issuers and protocols

| ID | Source | Page | Interval |
|---|---|---|---|
| S15 | Uniswap | [https://blog.uniswap.org/](https://blog.uniswap.org/) | 600 s |
| S16 | Ondo | [https://ondo.finance/blog](https://ondo.finance/blog) | 600 s |
| S17 | LayerZero | [https://layerzero.network/blog](https://layerzero.network/blog) | 600 s |
| S18 | BlackRock | [https://www.blackrock.com/corporate/newsroom/media/press-releases](https://www.blackrock.com/corporate/newsroom/media/press-releases) | 600 s |
| S19 | Franklin Templeton | [https://www.franklintempleton.com/press-releases](https://www.franklintempleton.com/press-releases) | 600 s |
| S20 | Circle | [https://www.circle.com/pressroom](https://www.circle.com/pressroom) | 600 s |
| S21 | Tether | [RSS](https://tether.io/feed/) | 300 s |
| S22 | Paxos | [https://www.paxos.com/newsroom](https://www.paxos.com/newsroom) | 600 s |

## Governance

| ID | Source | Links |
|---|---|---|
| S23 | 8 forums (latest topics RSS) | [Uniswap](https://gov.uniswap.org) · [Aave](https://governance.aave.com) · [Ethena](https://gov.ethenafoundation.com) · [CoW](https://forum.cow.fi) · [Morpho](https://forum.morpho.org) · [Euler](https://forum.euler.finance) · [1inch](https://gov.1inch.network) (moved from `gov.1inch.io`) · [NEAR](https://gov.near.org) |
| S24 | 6 Snapshot spaces | [uniswapgovernance.eth](https://snapshot.org/#/uniswapgovernance.eth) · [aavedao.eth](https://snapshot.org/#/aavedao.eth) · [cakevote.eth](https://snapshot.org/#/cakevote.eth) · [cow.eth](https://snapshot.org/#/cow.eth) · [morpho.eth](https://snapshot.org/#/morpho.eth) · [1inch.eth](https://snapshot.org/#/1inch.eth) |

## GitHub (S25)

| Repository | Watched |
|---|---|
| [Uniswap v4-core](https://github.com/Uniswap/v4-core/releases) | releases |
| [Aave v3 origin](https://github.com/aave-dao/aave-v3-origin/releases) | releases |
| [CoW services](https://github.com/cowprotocol/services/releases) | releases |
| [Hyperliquid Python SDK](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/releases) | releases |
| [1inch swap-vm](https://github.com/1inch/swap-vm/releases) | releases |
| [Lighter Python](https://github.com/elliottech/lighter-python/releases) | releases |
| [AvalancheGo](https://github.com/ava-labs/avalanchego/releases) | releases |
| [nearcore](https://github.com/near/nearcore/releases) | releases |
| [World Chain](https://github.com/worldcoin/world-chain/releases) | releases |
| [Plasma node templates](https://github.com/PlasmaLaboratories/node-templates/releases) | releases |
| [LayerZero v2](https://github.com/LayerZero-Labs/LayerZero-v2/commits/main) | commits |
| [Morpho Blue](https://github.com/morpho-org/morpho-blue/commits/main) | commits |

## Institutions

| ID | Source | Page | Interval |
|---|---|---|---|
| S26 | BNY | [https://www.bny.com/corporate/global/en/about-us/newsroom.html](https://www.bny.com/corporate/global/en/about-us/newsroom.html) | 600 s |
| S27 | The Clearing House | [https://www.theclearinghouse.org/About/Media/press-releases](https://www.theclearinghouse.org/About/Media/press-releases) | 600 s |
| S28 | Anchorage Digital | [https://www.anchorage.com/insights](https://www.anchorage.com/insights) | 600 s |

## News

| ID | Source | Feed | Interval |
|---|---|---|---|
| S30 | BBC | [business](https://feeds.bbci.co.uk/news/business/rss.xml) · [technology](https://feeds.bbci.co.uk/news/technology/rss.xml) | 120 s · 300 s |
| S31 | Bloomberg | [markets RSS](https://feeds.bloomberg.com/markets/news.rss) | 120 s |
| S32 | Reuters | [crypto via Google News RSS](https://news.google.com/rss/search?q=site:reuters.com+crypto+when:1d&hl=en-US&gl=US&ceid=US:en) | 300 s |
| S33 | CoinDesk | [RSS](https://www.coindesk.com/arc/outboundfeeds/rss/) | 120 s |
| S34 | The Block | [RSS](https://www.theblock.co/rss.xml) | 120 s |

## Market data (6 venues)

| Data | Venues and documentation |
|---|---|
| Spot | [Binance](https://developers.binance.com/docs/binance-spot-api-docs), [Coinbase](https://docs.cdp.coinbase.com/exchange/introduction/welcome), [OKX](https://www.okx.com/docs-v5/en/), [Bybit](https://bybit-exchange.github.io/docs/v5/intro), [Kraken](https://docs.kraken.com/api/) |
| Perpetuals | [Binance](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info), [OKX](https://www.okx.com/docs-v5/en/), [Bybit](https://bybit-exchange.github.io/docs/v5/intro), [Hyperliquid](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api) |
| Fees, unlocks, lending, stablecoins | [DefiLlama API](https://api-docs.defillama.com/) |
| Exchange wallets | [Binance](https://github.com/DefiLlama/DefiLlama-Adapters/blob/4a6ef31a4eed4f24882a7d82bff50306b40b6d6c/projects/binance/index.js) and [OKX](https://github.com/DefiLlama/DefiLlama-Adapters/blob/b70f0fb4dc9ff463866d676c14d21b0ed0930d76/projects/okex/index.js) address lists (DefiLlama-Adapters, pinned commits); balances via [Etherscan API](https://docs.etherscan.io/) |

## Universe (22 tokens)

| Basket | Tokens (official site) |
|---|---|
| Benchmarks | [BTC](https://bitcoin.org/), [ETH](https://ethereum.org/) |
| DEX & execution | [UNI](https://uniswap.org/), [CAKE](https://pancakeswap.finance/), [AERO](https://aerodrome.finance/), [CRV](https://www.curve.finance/), [1INCH](https://1inch.com/), [ZRX](https://0x.org/), [COW](https://cow.fi/) |
| Perp DEX | [HYPE](https://hyperliquid.xyz/), [LIT](https://lighter.xyz/) |
| Lending & credit | [AAVE](https://aave.com/), [MORPHO](https://morpho.org/), [EUL](https://www.euler.finance/) |
| RWA & tokenization | [ONDO](https://ondo.finance/) |
| Interoperability | [ZRO](https://layerzero.network/), [QNT](https://quant.network/) |
| L1 / settlement | [AVAX](https://www.avalanche.com/), [NEAR](https://near.org/), [XPL](https://www.plasma.org/) |
| Synthetic dollar | [ENA](https://ethena.fi/) |
| Identity / payments | [WLD](https://world.org/) |

## X (S29): 86 accounts in one private list, read every 60 s

### Organizations (41) and founders / C-level (37)

| Token | Organization accounts | Founders and C-level |
|---|---|---|
| BTC | [@bitcoincoreorg](https://x.com/bitcoincoreorg) | — (no founder account) |
| ETH | [@ethereum](https://x.com/ethereum), [@ethereumfndn](https://x.com/ethereumfndn) | [@VitalikButerin](https://x.com/VitalikButerin) (Vitalik Buterin, co-founder, EF board), [@AyaMiyagotchi](https://x.com/AyaMiyagotchi) (Aya Miyaguchi, EF President), [@aerugoettinea](https://x.com/aerugoettinea) (Bastian Aue, EF interim co-executive director) |
| UNI | [@Uniswap](https://x.com/Uniswap), [@UniswapFND](https://x.com/UniswapFND), [@unichain](https://x.com/unichain) | [@haydenzadams](https://x.com/haydenzadams) (Hayden Adams, founder, CEO Uniswap Labs), [@devinawalsh](https://x.com/devinawalsh) (Devin Walsh, executive director, Uniswap Foundation) |
| CAKE | [@PancakeSwap](https://x.com/PancakeSwap) | — (team is anonymous) |
| AERO | [@aeroxyz](https://x.com/aeroxyz), [@dromoslabs](https://x.com/dromoslabs), [@VelodromeFi](https://x.com/VelodromeFi) | [@wagmiAlexander](https://x.com/wagmiAlexander) (Alexander Cutler, co-founder, CEO Dromos Labs), [@jack_anorak](https://x.com/jack_anorak) (Chris Boulos, President, Dromos Labs), [@dannis19024](https://x.com/dannis19024) (David Annis, COO, Dromos Labs) |
| CRV | [@CurveFinance](https://x.com/CurveFinance) | [@newmichwill](https://x.com/newmichwill) (Michael Egorov, founder) |
| 1INCH | [@1inch](https://x.com/1inch) | [@deacix](https://x.com/deacix) (Sergej Kunz, co-founder, CEO) |
| ZRX | [@0xProject](https://x.com/0xProject), [@zeroexprotocol](https://x.com/zeroexprotocol), [@matchaxyz](https://x.com/matchaxyz) | [@abandeali1](https://x.com/abandeali1) (Amir Bandeali, co-founder, CEO), [@willwarren](https://x.com/willwarren) (Will Warren, co-founder, board) |
| COW | [@CoWSwap](https://x.com/CoWSwap) | [@AnnaMSGeorge](https://x.com/AnnaMSGeorge) (Anna George, co-founder), [@fleupold_](https://x.com/fleupold_) (Felix Leupold, technical lead) |
| HYPE | [@HyperliquidX](https://x.com/HyperliquidX), [@HyperFND](https://x.com/HyperFND) | [@chameleon_jeff](https://x.com/chameleon_jeff) (Jeff Yan, co-founder) |
| LIT | [@Lighter_xyz](https://x.com/Lighter_xyz) | [@vnovakovski](https://x.com/vnovakovski) (Vladimir Novakovski, founder, CEO) |
| AAVE | [@aave](https://x.com/aave) | [@StaniKulechov](https://x.com/StaniKulechov) (Stani Kulechov, founder, CEO) |
| MORPHO | [@Morpho](https://x.com/Morpho) | [@PaulFrambot](https://x.com/PaulFrambot) (Paul Frambot, co-founder, CEO), [@MerlinEgalite](https://x.com/MerlinEgalite) (Merlin Egalité, co-founder) |
| EUL | [@eulerfinance](https://x.com/eulerfinance) | [@0xJHan](https://x.com/0xJHan) (Jonathan Han, CEO), [@euler_mab](https://x.com/euler_mab) (Michael Bentley, co-founder) |
| ONDO | [@Ondo](https://x.com/Ondo), [@OndoFoundation](https://x.com/OndoFoundation), [@OndoPerps](https://x.com/OndoPerps) | [@iandebode](https://x.com/iandebode) (Ian De Bode, acting CEO and President) |
| ZRO | [@LayerZero_Core](https://x.com/LayerZero_Core), [@LayerZero_Labs](https://x.com/LayerZero_Labs), [@LayerZero_Fndn](https://x.com/LayerZero_Fndn), [@StargateFinance](https://x.com/StargateFinance) | [@PrimordialAA](https://x.com/PrimordialAA) (Bryan Pellegrino, co-founder, CEO), [@ryanzarick](https://x.com/ryanzarick) (Ryan Zarick, co-founder, CTO), [@caleb_banister](https://x.com/caleb_banister) (Caleb Banister, co-founder) |
| QNT | [@quantnetwork](https://x.com/quantnetwork) | [@gverdian](https://x.com/gverdian) (Gilbert Verdian, founder, CEO) |
| AVAX | [@avax](https://x.com/avax), [@AvaLabs](https://x.com/AvaLabs), [@AvalancheFDN](https://x.com/AvalancheFDN) | [@el33th4xor](https://x.com/el33th4xor) (Emin Gün Sirer, founder, CEO Ava Labs), [@CharleyCooper01](https://x.com/CharleyCooper01) (Charley Cooper, President, Ava Labs) |
| NEAR | [@NEARProtocol](https://x.com/NEARProtocol), [@near_ai](https://x.com/near_ai) | [@ilblackdragon](https://x.com/ilblackdragon) (Illia Polosukhin, co-founder), [@AlexSkidanov](https://x.com/AlexSkidanov) (Alexander Skidanov, co-founder), [@georgexzeng](https://x.com/georgexzeng) (George Zeng, CPO) |
| XPL | [@Plasma](https://x.com/Plasma) | [@pauliepunt](https://x.com/pauliepunt) (Paul Faecks, founder, CEO), [@C_Angermayer](https://x.com/C_Angermayer) (Christian Angermayer, co-founder) |
| ENA | [@ethena](https://x.com/ethena) | [@gdog97_](https://x.com/gdog97_) (Guy Young, founder, CEO) |
| WLD | [@worldnetwork](https://x.com/worldnetwork), [@tfh_technology](https://x.com/tfh_technology), [@worldcoinfnd](https://x.com/worldcoinfnd), [@world_chain_](https://x.com/world_chain_) | [@alexblania](https://x.com/alexblania) (Alex Blania, co-founder, CEO Tools for Humanity), [@sama](https://x.com/sama) (Sam Altman, co-founder, chairman Tools for Humanity), [@tiagosada](https://x.com/tiagosada) (Tiago Sada, CPO, Tools for Humanity) |

### Other (8)

| Kind | Accounts |
|---|---|
| Coinbase listings | [@CoinbaseMarkets](https://x.com/CoinbaseMarkets) |
| Tokenized-asset issuer | [@Securitize](https://x.com/Securitize) |
| Reporters and alert accounts | [@EleanorTerrett](https://x.com/EleanorTerrett), [@WuBlockchain](https://x.com/WuBlockchain), [@tier10k](https://x.com/tier10k), [@zachxbt](https://x.com/zachxbt), [@PeckShieldAlert](https://x.com/PeckShieldAlert), [@lookonchain](https://x.com/lookonchain) |
