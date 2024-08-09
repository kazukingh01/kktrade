--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1 (Debian 16.1-1.pgdg120+1)
-- Dumped by pg_dump version 16.1 (Debian 16.1-1.pgdg120+1)

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
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.master_symbol OWNER TO postgres;

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
-- Name: master_symbol trg_update_sys_updated_master_symbol; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_update_sys_updated_master_symbol BEFORE UPDATE ON public.master_symbol FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();


--
-- PostgreSQL database dump complete
--

