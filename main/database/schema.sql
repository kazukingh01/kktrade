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

CREATE TABLE public.bitflyer_executions (
    symbol smallint NOT NULL,
    id bigint NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bitflyer_executions OWNER TO postgres;

--
-- Name: bitflyer_orderbook; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_orderbook (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    side smallint NOT NULL,
    price integer,
    size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bitflyer_orderbook OWNER TO postgres;

--
-- Name: bitflyer_ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_ticker (
    symbol smallint NOT NULL,
    tick_id integer NOT NULL,
    state smallint,
    unixtime bigint,
    bid integer,
    ask integer,
    bid_size integer,
    ask_size integer,
    total_bid_depth integer,
    total_ask_depth integer,
    market_bid_size integer,
    market_ask_size integer,
    last_traded_price integer,
    volume bigint,
    volume_by_product bigint,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bitflyer_ticker OWNER TO postgres;

--
-- Name: bybit_executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions OWNER TO postgres;

--
-- Name: bybit_executions_2019; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions_2019 (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions_2019 OWNER TO postgres;

--
-- Name: bybit_executions_2020; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions_2020 (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions_2020 OWNER TO postgres;

--
-- Name: bybit_executions_2021; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions_2021 (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions_2021 OWNER TO postgres;

--
-- Name: bybit_executions_2022; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions_2022 (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions_2022 OWNER TO postgres;

--
-- Name: bybit_executions_2023; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions_2023 (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions_2023 OWNER TO postgres;

--
-- Name: bybit_kline; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_kline (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    kline_type smallint NOT NULL,
    "interval" smallint NOT NULL,
    price_open integer,
    price_high integer,
    price_low integer,
    price_close integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_kline OWNER TO postgres;

--
-- Name: bybit_orderbook; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_orderbook (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    side smallint NOT NULL,
    price integer,
    size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_orderbook OWNER TO postgres;

--
-- Name: bybit_ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_ticker (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    bid integer,
    ask integer,
    bid_size integer,
    ask_size integer,
    last_traded_price integer,
    index_price integer,
    mark_price integer,
    volume bigint,
    turnover bigint,
    open_interest bigint,
    open_interest_value bigint,
    funding_rate smallint,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_ticker OWNER TO postgres;

--
-- Name: dukascopy_ohlcv; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dukascopy_ohlcv (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    "interval" smallint NOT NULL,
    price_open integer,
    price_high integer,
    price_low integer,
    price_close integer,
    ask_volume integer,
    bid_volume integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.dukascopy_ohlcv OWNER TO postgres;

--
-- Name: dukascopy_ticks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dukascopy_ticks (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    bid integer,
    ask integer,
    bid_size integer,
    ask_size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.dukascopy_ticks OWNER TO postgres;

--
-- Name: eodhd_ohlcv; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.eodhd_ohlcv (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    "interval" smallint NOT NULL,
    price_open integer,
    price_high integer,
    price_low integer,
    price_close integer,
    volume bigint,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.eodhd_ohlcv OWNER TO postgres;

--
-- Name: master_symbol; Type: TABLE; Schema: public; Owner: postgres
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

--
-- Name: bitflyer_executions bitflyer_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_executions
    ADD CONSTRAINT bitflyer_executions_pkey PRIMARY KEY (symbol, id);


--
-- Name: bitflyer_ticker bitflyer_ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_ticker
    ADD CONSTRAINT bitflyer_ticker_pkey PRIMARY KEY (symbol, tick_id);


--
-- Name: bybit_executions_2019 bybit_executions_2019_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions_2019
    ADD CONSTRAINT bybit_executions_2019_pkey PRIMARY KEY (symbol, id);


--
-- Name: bybit_executions_2020 bybit_executions_2020_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions_2020
    ADD CONSTRAINT bybit_executions_2020_pkey PRIMARY KEY (symbol, id);


--
-- Name: bybit_executions_2021 bybit_executions_2021_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions_2021
    ADD CONSTRAINT bybit_executions_2021_pkey PRIMARY KEY (symbol, id);


--
-- Name: bybit_executions_2022 bybit_executions_2022_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions_2022
    ADD CONSTRAINT bybit_executions_2022_pkey PRIMARY KEY (symbol, id);


--
-- Name: bybit_executions_2023 bybit_executions_2023_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions_2023
    ADD CONSTRAINT bybit_executions_2023_pkey PRIMARY KEY (symbol, id);


--
-- Name: bybit_executions bybit_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions
    ADD CONSTRAINT bybit_executions_pkey PRIMARY KEY (symbol, id);


--
-- Name: bybit_kline bybit_kline_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_kline
    ADD CONSTRAINT bybit_kline_pkey PRIMARY KEY (symbol, unixtime, kline_type, "interval");


--
-- Name: bybit_ticker bybit_ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_ticker
    ADD CONSTRAINT bybit_ticker_pkey PRIMARY KEY (symbol, unixtime);


--
-- Name: dukascopy_ohlcv dukascopy_ohlcv_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dukascopy_ohlcv
    ADD CONSTRAINT dukascopy_ohlcv_pkey PRIMARY KEY (symbol, unixtime, "interval");


--
-- Name: dukascopy_ticks dukascopy_ticks_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dukascopy_ticks
    ADD CONSTRAINT dukascopy_ticks_pkey PRIMARY KEY (symbol, unixtime);


--
-- Name: eodhd_ohlcv eodhd_ohlcv_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.eodhd_ohlcv
    ADD CONSTRAINT eodhd_ohlcv_pkey PRIMARY KEY (symbol, unixtime, "interval");


--
-- Name: master_symbol master_symbol_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.master_symbol
    ADD CONSTRAINT master_symbol_pkey PRIMARY KEY (symbol_id);


--
-- Name: master_symbol master_symbol_unique_0; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.master_symbol
    ADD CONSTRAINT master_symbol_unique_0 UNIQUE (exchange, symbol_name);


--
-- Name: bitflyer_executions_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_0 ON public.bitflyer_executions USING btree (symbol);


--
-- Name: bitflyer_executions_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_1 ON public.bitflyer_executions USING btree (id);


--
-- Name: bitflyer_executions_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_2 ON public.bitflyer_executions USING btree (unixtime);


--
-- Name: bitflyer_orderbook_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_orderbook_0 ON public.bitflyer_orderbook USING btree (unixtime);


--
-- Name: bitflyer_ticker_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_0 ON public.bitflyer_ticker USING btree (symbol);


--
-- Name: bitflyer_ticker_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_1 ON public.bitflyer_ticker USING btree (tick_id);


--
-- Name: bitflyer_ticker_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_2 ON public.bitflyer_ticker USING btree (unixtime);


--
-- Name: bybit_executions_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_executions_0 ON public.bybit_executions USING btree (symbol);


--
-- Name: bybit_executions_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_executions_1 ON public.bybit_executions USING btree (id);


--
-- Name: bybit_executions_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_executions_2 ON public.bybit_executions USING btree (unixtime);


--
-- Name: bybit_kline_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_kline_0 ON public.bybit_kline USING btree (symbol);


--
-- Name: bybit_kline_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_kline_1 ON public.bybit_kline USING btree (unixtime);


--
-- Name: bybit_kline_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_kline_2 ON public.bybit_kline USING btree (kline_type);


--
-- Name: bybit_kline_3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_kline_3 ON public.bybit_kline USING btree ("interval");


--
-- Name: bybit_orderbook_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_orderbook_0 ON public.bybit_orderbook USING btree (unixtime);


--
-- Name: bybit_ticker_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_ticker_0 ON public.bybit_ticker USING btree (symbol);


--
-- Name: bybit_ticker_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_ticker_1 ON public.bybit_ticker USING btree (unixtime);


--
-- Name: dukascopy_ohlcv_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dukascopy_ohlcv_0 ON public.dukascopy_ohlcv USING btree (symbol);


--
-- Name: dukascopy_ohlcv_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dukascopy_ohlcv_1 ON public.dukascopy_ohlcv USING btree (unixtime);


--
-- Name: dukascopy_ticks_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dukascopy_ticks_0 ON public.dukascopy_ticks USING btree (symbol);


--
-- Name: dukascopy_ticks_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dukascopy_ticks_1 ON public.dukascopy_ticks USING btree (unixtime);


--
-- Name: eodhd_ohlcv_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX eodhd_ohlcv_0 ON public.eodhd_ohlcv USING btree (symbol);


--
-- Name: eodhd_ohlcv_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX eodhd_ohlcv_1 ON public.eodhd_ohlcv USING btree (unixtime);


--
-- Name: eodhd_ohlcv_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX eodhd_ohlcv_2 ON public.eodhd_ohlcv USING btree ("interval");


--
-- Name: bitflyer_executions trg_update_sys_updated_bitflyer_executions; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bitflyer_executions BEFORE UPDATE ON public.bitflyer_executions FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bitflyer_orderbook trg_update_sys_updated_bitflyer_orderbook; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bitflyer_orderbook BEFORE UPDATE ON public.bitflyer_orderbook FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bitflyer_ticker trg_update_sys_updated_bitflyer_ticker; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bitflyer_ticker BEFORE UPDATE ON public.bitflyer_ticker FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_executions trg_update_sys_updated_bybit_executions; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions BEFORE UPDATE ON public.bybit_executions FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_executions_2019 trg_update_sys_updated_bybit_executions_2019; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions_2019 BEFORE UPDATE ON public.bybit_executions_2019 FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_executions_2020 trg_update_sys_updated_bybit_executions_2020; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions_2020 BEFORE UPDATE ON public.bybit_executions_2020 FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_executions_2021 trg_update_sys_updated_bybit_executions_2021; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions_2021 BEFORE UPDATE ON public.bybit_executions_2021 FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_executions_2022 trg_update_sys_updated_bybit_executions_2022; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions_2022 BEFORE UPDATE ON public.bybit_executions_2022 FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_executions_2023 trg_update_sys_updated_bybit_executions_2023; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions_2023 BEFORE UPDATE ON public.bybit_executions_2023 FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_kline trg_update_sys_updated_bybit_kline; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_kline BEFORE UPDATE ON public.bybit_kline FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_orderbook trg_update_sys_updated_bybit_orderbook; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_orderbook BEFORE UPDATE ON public.bybit_orderbook FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: bybit_ticker trg_update_sys_updated_bybit_ticker; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_ticker BEFORE UPDATE ON public.bybit_ticker FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: dukascopy_ticks trg_update_sys_updated_dukascopy_ticks; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_dukascopy_ticks BEFORE UPDATE ON public.dukascopy_ticks FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: eodhd_ohlcv trg_update_sys_updated_eodhd_ohlcv; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_eodhd_ohlcv BEFORE UPDATE ON public.eodhd_ohlcv FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: master_symbol trg_update_sys_updated_master_symbol; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_master_symbol BEFORE UPDATE ON public.master_symbol FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- PostgreSQL database dump complete
--

