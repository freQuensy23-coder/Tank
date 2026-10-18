CREATE INDEX trades_instrument_time_key ON mkt.trades(instrument_id,ts,storage_key);
CREATE INDEX klines_instrument_time_key ON mkt.klines_1m(instrument_id,open_time,storage_key);
CREATE INDEX flow_instrument_time_key ON mkt.flow_bars_1m(instrument_id,minute_start,storage_key);
CREATE INDEX books_instrument_time_key ON mkt.book_summaries(instrument_id,exchange_ts,storage_key);
CREATE INDEX oi_instrument_time_key ON mkt.oi_obs(instrument_id,ts,storage_key);
CREATE INDEX liq_instrument_time_key ON mkt.liq_obs(instrument_id,ts,storage_key);
CREATE INDEX funding_instrument_time_key ON mkt.funding_obs(instrument_id,as_of,storage_key);
