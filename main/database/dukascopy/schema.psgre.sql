
--
-- Name: dukascopy_ohlcv; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dukascopy_ohlcv (
    symbol smallint NOT NULL,
    unixtime timestamp with time zone NOT NULL,
    "interval" smallint NOT NULL,
    price_open real,
    price_high real,
    price_low real,
    price_close real,
    ask_volume real,
    bid_volume real
);


ALTER TABLE public.dukascopy_ohlcv OWNER TO postgres;

--
-- Name: dukascopy_ticks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dukascopy_ticks (
    symbol smallint NOT NULL,
    unixtime timestamp with time zone NOT NULL,
    bid real,
    ask real,
    bid_size real,
    ask_size real
);


ALTER TABLE public.dukascopy_ticks OWNER TO postgres;


--
-- Name: dukascopy_ohlcv dukascopy_ohlcv_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dukascopy_ohlcv
    ADD CONSTRAINT dukascopy_ohlcv_pkey PRIMARY KEY (symbol, unixtime, "interval");


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
