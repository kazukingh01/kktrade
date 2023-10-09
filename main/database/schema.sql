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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: orderbook; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_orderbook (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    type character varying(4) NOT NULL,
    price integer,
    size integer,
    scale smallint NOT NULL,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bitflyer_orderbook OWNER TO postgres;

--
-- Name: executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_executions (
    symbol smallint NOT NULL,
    id bigint NOT NULL,
    type character varying(4) NOT NULL,
    scale smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bitflyer_executions OWNER TO postgres;

--
-- Name: ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_ticker (
    symbol smallint NOT NULL,
    tick_id integer NOT NULL,
    state smallint,
    scale smallint NOT NULL,
    unixtime bigint,
    best_bid integer,
    best_ask integer,
    best_bid_size integer,
    best_ask_size integer,
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
-- Name: executions executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_executions
    ADD CONSTRAINT bitflyer_executions_pkey PRIMARY KEY (symbol, id);


--
-- Name: ticker ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_ticker
    ADD CONSTRAINT bitflyer_ticker_pkey PRIMARY KEY (symbol, tick_id);


--
-- Name: orderbook_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_orderbook_0 ON public.bitflyer_orderbook USING btree (unixtime);


--
-- Name: executions_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_0 ON public.bitflyer_executions USING btree (symbol);


--
-- Name: executions_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_1 ON public.bitflyer_executions USING btree (id);


--
-- Name: executions_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_2 ON public.bitflyer_executions USING btree (unixtime);


--
-- Name: ticker_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_0 ON public.bitflyer_ticker USING btree (symbol);


--
-- Name: ticker_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_1 ON public.bitflyer_ticker USING btree (tick_id);


--
-- Name: ticker_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_2 ON public.bitflyer_ticker USING btree (unixtime);


--
-- Name: orderbook trg_update_sys_updated_orderbook; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bitflyer_orderbook BEFORE UPDATE ON public.bitflyer_orderbook FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: executions trg_update_sys_updated_executions; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bitflyer_executions BEFORE UPDATE ON public.bitflyer_executions FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: ticker trg_update_sys_updated_ticker; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bitflyer_ticker BEFORE UPDATE ON public.bitflyer_ticker FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();

--
-- Name: orderbook; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_orderbook (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    type character varying(4) NOT NULL,
    price integer,
    size integer,
    scale smallint NOT NULL,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_orderbook OWNER TO postgres;

--
-- Name: executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_executions (
    symbol smallint NOT NULL,
    id character varying(36) NOT NULL,
    type character varying(4) NOT NULL,
    scale smallint NOT NULL,
    unixtime bigint,
    price integer,
    size integer,
    is_block_trade boolean,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.bybit_executions OWNER TO postgres;

--
-- Name: ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bybit_ticker (
    symbol smallint NOT NULL,
    scale smallint NOT NULL,
    unixtime bigint,
    best_bid integer,
    best_ask integer,
    best_bid_size integer,
    best_ask_size integer,
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
-- Name: executions executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_executions
    ADD CONSTRAINT bybit_executions_pkey PRIMARY KEY (symbol, id);


--
-- Name: ticker ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bybit_ticker
    ADD CONSTRAINT bybit_ticker_pkey PRIMARY KEY (symbol, unixtime);


--
-- Name: orderbook_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_orderbook_0 ON public.bybit_orderbook USING btree (unixtime);


--
-- Name: executions_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_executions_0 ON public.bybit_executions USING btree (symbol);


--
-- Name: executions_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_executions_1 ON public.bybit_executions USING btree (id);


--
-- Name: executions_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_executions_2 ON public.bybit_executions USING btree (unixtime);


--
-- Name: ticker_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_ticker_0 ON public.bybit_ticker USING btree (symbol);


--
-- Name: ticker_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bybit_ticker_1 ON public.bybit_ticker USING btree (unixtime);


--
-- Name: orderbook trg_update_sys_updated_orderbook; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_orderbook BEFORE UPDATE ON public.bybit_orderbook FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: executions trg_update_sys_updated_executions; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_executions BEFORE UPDATE ON public.bybit_executions FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: ticker trg_update_sys_updated_ticker; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_bybit_ticker BEFORE UPDATE ON public.bybit_ticker FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- PostgreSQL database dump complete
--

