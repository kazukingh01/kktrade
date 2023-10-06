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

CREATE TABLE public.orderbook (
    symbol character varying(10) NOT NULL,
    unixtime bigint NOT NULL,
    type character varying(4) NOT NULL,
    price integer,
    size integer,
    scale smallint,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.orderbook OWNER TO postgres;

--
-- Name: executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.executions (
    symbol character varying(10) NOT NULL,
    id bigint NOT NULL,
    type character varying(4) NOT NULL,
    scale smallint,
    unixtime bigint,
    price integer,
    size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.executions OWNER TO postgres;

--
-- Name: ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ticker (
    symbol character varying(10) NOT NULL,
    tick_id integer NOT NULL,
    state smallint,
    scale smallint,
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


ALTER TABLE public.ticker OWNER TO postgres;

--
-- Name: executions executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.executions
    ADD CONSTRAINT executions_pkey PRIMARY KEY (symbol, id);


--
-- Name: ticker ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT ticker_pkey PRIMARY KEY (symbol, tick_id);


--
-- Name: orderbook_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX orderbook_0 ON public.orderbook USING btree (unixtime);


--
-- Name: executions_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX executions_0 ON public.executions USING btree (symbol);


--
-- Name: executions_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX executions_1 ON public.executions USING btree (id);


--
-- Name: executions_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX executions_2 ON public.executions USING btree (unixtime);


--
-- Name: ticker_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ticker_0 ON public.ticker USING btree (symbol);


--
-- Name: ticker_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ticker_1 ON public.ticker USING btree (tick_id);


--
-- Name: ticker_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ticker_2 ON public.ticker USING btree (unixtime);


--
-- Name: orderbook trg_update_sys_updated_orderbook; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_orderbook BEFORE UPDATE ON public.orderbook FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: executions trg_update_sys_updated_executions; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_executions BEFORE UPDATE ON public.executions FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- Name: ticker trg_update_sys_updated_ticker; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_ticker BEFORE UPDATE ON public.ticker FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- PostgreSQL database dump complete
--

