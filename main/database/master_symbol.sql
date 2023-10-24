--
-- PostgreSQL database dump
--

-- Dumped from database version 16.0 (Ubuntu 16.0-1.pgdg22.04+1)
-- Dumped by pg_dump version 16.0 (Ubuntu 16.0-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: update_sys_updated(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_sys_updated() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.sys_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_sys_updated() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: bitflyer_executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.master_symbol (
    symbol_id smallint NOT NULL,
    symbol_name character varying(30) NOT NULL,
    exchange character varying(30) NOT NULL,
    base character varying(30) NOT NULL,
    currency character varying(30) NOT NULL,
    is_active boolean NOT NULL,
    explain text,
    scale_pre json,
    scale_aft json,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

ALTER TABLE public.master_symbol OWNER TO postgres;
ALTER TABLE ONLY public.master_symbol ADD CONSTRAINT master_symbol_pkey PRIMARY KEY (symbol_id);
ALTER TABLE ONLY public.master_symbol ADD CONSTRAINT master_symbol_pkey PRIMARY KEY (exchange, symbol_name);

CREATE TRIGGER trg_update_sys_updated_master_symbol BEFORE UPDATE ON public.master_symbol FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();

INSERT INTO public.master_symbol (symbol_id, symbol_name, exchange, base, currency, is_active, explain, scale_pre, scale_aft)
VALUES
    ( 0,        'BTC_JPY',  'bitflyer',      'BTC',  'JPY', true,  null, '{"price":     1, "size":  100000, "best_bid":     1, "best_ask":     1, "ltp":    1, "best_bid_size": 100000, "best_ask_size": 100000, "total_bid_depth": 100000, "total_ask_depth": 100000, "market_bid_size": 100000, "market_ask_size": 100000, "volume": 100000, "volume_by_product": 100000}', null),
    ( 1,        'XRP_JPY',  'bitflyer',      'XRP',  'JPY', true,  null, '{"price":   100, "size":     100, "best_bid":   100, "best_ask":   100, "ltp":  100, "best_bid_size":    100, "best_ask_size":    100, "total_bid_depth":    100, "total_ask_depth":    100, "market_bid_size":    100, "market_ask_size":    100, "volume":    100, "volume_by_product":    100}', null),
    ( 2,        'ETH_JPY',  'bitflyer',      'ETH',  'JPY', true,  null, '{"price":     1, "size":  100000, "best_bid":     1, "best_ask":     1, "ltp":    1, "best_bid_size": 100000, "best_ask_size": 100000, "total_bid_depth": 100000, "total_ask_depth": 100000, "market_bid_size": 100000, "market_ask_size": 100000, "volume": 100000, "volume_by_product": 100000}', null),
    ( 3,        'XLM_JPY',  'bitflyer',      'XLM',  'JPY', false, null, '{"price":  1000, "size":     100, "best_bid":  1000, "best_ask":  1000, "ltp": 1000, "best_bid_size":    100, "best_ask_size":    100, "total_bid_depth":    100, "total_ask_depth":    100, "market_bid_size":    100, "market_ask_size":    100, "volume":    100, "volume_by_product":    100}', null),
    ( 4,       'MONA_JPY',  'bitflyer',     'MONA',  'JPY', false, null, '{"price":  1000, "size":     100, "best_bid":  1000, "best_ask":  1000, "ltp": 1000, "best_bid_size":    100, "best_ask_size":    100, "total_bid_depth":    100, "total_ask_depth":    100, "market_bid_size":    100, "market_ask_size":    100, "volume":    100, "volume_by_product":    100}', null),
    ( 5,     'FX_BTC_JPY',  'bitflyer',   'BTC_FX',  'JPY', true,  null, '{"price":     1, "size":  100000, "best_bid":     1, "best_ask":     1, "ltp":    1, "best_bid_size": 100000, "best_ask_size": 100000, "total_bid_depth": 100000, "total_ask_depth": 100000, "market_bid_size": 100000, "market_ask_size": 100000, "volume": 100000, "volume_by_product": 100000}', null),
    ( 6,   'spot@BTCUSDT',     'bybit',      'BTC', 'USDT', true,  null, '{"price":   100, "size": 1000000, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size": 1000000, "best_ask_size": 1000000, "volume": 1000000, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    ( 7,   'spot@ETHUSDC',     'bybit',      'ETH', 'USDC', true,  null, '{"price":   100, "size":  100000, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size":  100000, "best_ask_size":  100000, "volume":  100000, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    ( 8,   'spot@BTCUSDC',     'bybit',      'BTC', 'USDC', true,  null, '{"price":   100, "size": 1000000, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size": 1000000, "best_ask_size": 1000000, "volume": 1000000, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    ( 9,   'spot@ETHUSDT',     'bybit',      'ETH', 'USDT', true,  null, '{"price":   100, "size":  100000, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size":  100000, "best_ask_size":  100000, "volume":  100000, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    (10,   'spot@XRPUSDT',     'bybit',      'XRP', 'USDT', true,  null, '{"price": 10000, "size":     100, "best_bid": 10000, "best_ask": 10000, "last_traded_price": 10000, "index_price": 10000, "mark_price": 10000, "funding_rate": 10000, "best_bid_size":     100, "best_ask_size":     100, "volume":     100, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open": 10000, "price_high": 10000, "price_low": 10000, "price_close": 10000}', null),
    (11, 'linear@BTCUSDT',     'bybit',      'BTC', 'USDT', true,  null, '{"price":   100, "size":    1000, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size":    1000, "best_ask_size":    1000, "volume":    1000, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    (12, 'linear@ETHUSDT',     'bybit',      'ETH', 'USDT', true,  null, '{"price":   100, "size":     100, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size":     100, "best_ask_size":     100, "volume":     100, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    (13, 'linear@XRPUSDT',     'bybit',      'XRP', 'USDT', true,  null, '{"price": 10000, "size":       1, "best_bid": 10000, "best_ask": 10000, "last_traded_price": 10000, "index_price": 10000, "mark_price": 10000, "funding_rate": 10000, "best_bid_size":       1, "best_ask_size":       1, "volume":       1, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open": 10000, "price_high": 10000, "price_low": 10000, "price_close": 10000}', null),
    (14, 'inverse@BTCUSD',     'bybit',      'BTC',  'USD', true,  null, '{"price":    10, "size":       1, "best_bid":    10, "best_ask":    10, "last_traded_price":    10, "index_price":    10, "mark_price":    10, "funding_rate":    10, "best_bid_size":       1, "best_ask_size":       1, "volume":       1, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":    10, "price_high":    10, "price_low":    10, "price_close":    10}', null),
    (15, 'inverse@ETHUSD',     'bybit',      'ETH',  'USD', true,  null, '{"price":   100, "size":       1, "best_bid":   100, "best_ask":   100, "last_traded_price":   100, "index_price":   100, "mark_price":   100, "funding_rate":   100, "best_bid_size":       1, "best_ask_size":       1, "volume":       1, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open":   100, "price_high":   100, "price_low":   100, "price_close":   100}', null),
    (16, 'inverse@XRPUSD',     'bybit',      'XRP',  'USD', true,  null, '{"price": 10000, "size":       1, "best_bid": 10000, "best_ask": 10000, "last_traded_price": 10000, "index_price": 10000, "mark_price": 10000, "funding_rate": 10000, "best_bid_size":       1, "best_ask_size":       1, "volume":       1, "open_interest": 1, "open_interest_value": 1, "turnover": 1, "price_open": 10000, "price_high": 10000, "price_low": 10000, "price_close": 10000}', null),
    (17,   'USDJPY.FOREX',     'eodhd',      'USD',  'JPY', true,  null, '{"price_open":   1000, "price_high":   1000, "price_low":   1000, "price_close":   1000, "volume": 1}', null),
    (18,   'EURUSD.FOREX',     'eodhd',      'EUR',  'USD', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (19,   'GBPUSD.FOREX',     'eodhd',      'GBP',  'USD', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (20,   'USDCHF.FOREX',     'eodhd',      'USD',  'CHF', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (21,   'AUDUSD.FOREX',     'eodhd',      'AUD',  'USD', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (22,   'USDCAD.FOREX',     'eodhd',      'USD',  'CAD', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (23,   'NZDUSD.FOREX',     'eodhd',      'NZD',  'USD', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (24,   'EURGBP.FOREX',     'eodhd',      'EUR',  'GBP', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (25,   'EURJPY.FOREX',     'eodhd',      'EUR',  'JPY', true,  null, '{"price_open":   1000, "price_high":   1000, "price_low":   1000, "price_close":   1000, "volume": 1}', null),
    (26,   'EURCHF.FOREX',     'eodhd',      'EUR',  'CHF', true,  null, '{"price_open": 100000, "price_high": 100000, "price_low": 100000, "price_close": 100000, "volume": 1}', null),
    (27,   'XAUUSD.FOREX',     'eodhd',      'XAU',  'USD', true,  null, '{"price_open":    100, "price_high":    100, "price_low":    100, "price_close":    100, "volume": 1}', null),
    (28,      'GSPC.INDX',     'eodhd',     'GSPC',  'USD', true,  'S&P 500',               '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (29,       'DJI.INDX',     'eodhd',      'DJI',  'USD', true,  'Dow Jones',             '{"price_open":    1000, "price_high":    1000, "price_low":    1000, "price_close":    1000, "volume": 1}', null),
    (30,      'IXIC.INDX',     'eodhd',     'IXIC',  'USD', true,  'NASDAQ Index',          '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (31,   'BUK100P.INDX',     'eodhd',  'BUK100P',  'GBP', true,  'Cboe UK 100',           '{"price_open":  100000, "price_high":  100000, "price_low":  100000, "price_close":  100000, "volume": 1}', null),
    (32,       'VIX.INDX',     'eodhd',      'VIX',  'USD', true,  'CBOE Volatility Index', '{"price_open":     100, "price_high":     100, "price_low":     100, "price_close":     100, "volume": 1}', null),
    (33,     'GDAXI.INDX',     'eodhd',    'GDAXI',  'EUR', true,  'Germany 40 index',      '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (34,      'FCHI.INDX',     'eodhd',     'FCHI',  'EUR', true,  'France 40 index',       '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (35,  'STOXX50E.INDX',     'eodhd', 'STOXX50E',  'EUR', true,  'Europe 50 index',       '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (36,      'N100.INDX',     'eodhd',     'N100',  'EUR', true,  'Euronext 100 Index',    '{"price_open":  100000, "price_high":  100000, "price_low":  100000, "price_close":  100000, "volume": 1}', null),
    (37,       'BFX.INDX',     'eodhd',      'BFX',  'EUR', true,  'Brussels 20 Index',     '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (38,     'IMOEX.INDX',     'eodhd',    'IMOEX',  'RUB', false, 'Russia 50 Index',       null, null),
    (39,      'N225.INDX',     'eodhd',     'N225',  'JPY', true,  'Nikkei 225',            '{"price_open":    1000, "price_high":    1000, "price_low":    1000, "price_close":    1000, "volume": 1}', null),
    (40,       'HSI.INDX',     'eodhd',      'HSI',  'HKD', true,  'Hong Kong 40 Index',    '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (41,      'SSEC.INDX',     'eodhd',     'SSEC',     '', false, null,                    null, null),
    (42,      'AORD.INDX',     'eodhd',     'AORD',  'AUD', true,  'Australlia 500 index',  '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (43,     'BSESN.INDX',     'eodhd',    'BSESN',  'INR', true,  'india 30 index',        '{"price_open":    1000, "price_high":    1000, "price_low":    1000, "price_close":    1000, "volume": 1}', null),
    (44,      'JKSE.INDX',     'eodhd',     'JKSE',  'IDR', true,  'Jakarta all Index',     '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (45,      'NZ50.INDX',     'eodhd',     'NZ50',  'NZD', true,  'New Zealand 50 Index',  '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (46,      'KS11.INDX',     'eodhd',     'KS11',  'KRW', true,  'Koria Index',           '{"price_open":  100000, "price_high":  100000, "price_low":  100000, "price_close":  100000, "volume": 1}', null),
    (47,      'TWII.INDX',     'eodhd',     'TWII',  'TWD', true,  'Taiwan Index',          '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (48,    'GSPTSE.INDX',     'eodhd',   'GSPTSE',  'CAD', true,  'Canada 250 Index',      '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (49,      'BVSP.INDX',     'eodhd',     'BVSP',  'BRL', true,  'Brazil index',          '{"price_open":    1000, "price_high":    1000, "price_low":    1000, "price_close":    1000, "volume": 1}', null),
    (50,       'MXX.INDX',     'eodhd',   'GSPTSE',  'MXN', true,  'Mexico Index',          '{"price_open":   10000, "price_high":   10000, "price_low":   10000, "price_close":   10000, "volume": 1}', null),
    (51,    'SPIPSA.INDX',     'eodhd',   'SPIPSA',  'CLP', false, 'Chile Index',           null, null),
    (52,      'MERV.INDX',     'eodhd',     'MERV',  'ARS', false, 'Argentina  Index',      null, null),
    (53,     'TA125.INDX',     'eodhd',    'TA125',  'ILS', false, 'Israel 125 Index',      null, null),
    (54,   'USA500IDXUSD', 'dukascopy',     'GSPC',  'USD', true,  'S&P 500',               '{"ask": 1, "bid": 1, "ask_size":  100000, "bid_size":  100000}', '{"ask": 0.001, "bid": 0.001}'),
    (55,      'VOLIDXUSD', 'dukascopy',      'VIX',  'USD', true,  'CBOE Volatility Index', '{"ask": 1, "bid": 1, "ask_size":   10000, "bid_size":   10000}', '{"ask":  0.01, "bid":  0.01}'),
    (56,      'CHIIDXUSD', 'dukascopy',     'XIN9',  'CNY', true,  'China A50 Index',       '{"ask": 1, "bid": 1, "ask_size": 1000000, "bid_size": 1000000}', '{"ask": 0.001, "bid": 0.001}'),
    (57,      'HKGIDXHKD', 'dukascopy',      'HSI',  'HKD', true,  'Hong Kong 40 Index',    '{"ask": 1, "bid": 1, "ask_size": 1000000, "bid_size": 1000000}', '{"ask": 0.001, "bid": 0.001}'),
    (58,      'JPNIDXJPY', 'dukascopy',     'N225',  'JPY', true,  'Nikkei 225',            '{"ask": 1, "bid": 1, "ask_size":    1000, "bid_size":    1000}', '{"ask": 0.001, "bid": 0.001}'),
    (59,      'AUSIDXAUD', 'dukascopy',     'AXJO',  'AUD', true,  'Australlia 200 index',  '{"ask": 1, "bid": 1, "ask_size":  100000, "bid_size":  100000}', '{"ask": 0.001, "bid": 0.001}'),
    (60,      'INDIDXUSD', 'dukascopy',     'NSEI',  'INR', true,  'India 50 index',        '{"ask": 1, "bid": 1, "ask_size": 1000000, "bid_size": 1000000}', '{"ask": 0.001, "bid": 0.001}'),
    (61,      'SGDIDXSGD', 'dukascopy',    'SSGF3',  'SGD', true,  'Singapore Blue Chip',   '{"ask": 1, "bid": 1, "ask_size":   10000, "bid_size":   10000}', '{"ask": 0.001, "bid": 0.001}'),
    (62,      'FRAIDXEUR', 'dukascopy',     'FCHI',  'EUR', true,  'France 40 index',       '{"ask": 1, "bid": 1, "ask_size": 1000000, "bid_size": 1000000}', '{"ask": 0.001, "bid": 0.001}')
;