--
-- Name: eodhd_ohlcv; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.eodhd_ohlcv (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    "interval" smallint NOT NULL,
    price_open real,
    price_high real,
    price_low real,
    price_close real,
    volume real
);


ALTER TABLE public.eodhd_ohlcv OWNER TO postgres;

--
-- Name: eodhd_ohlcv eodhd_ohlcv_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.eodhd_ohlcv
    ADD CONSTRAINT eodhd_ohlcv_pkey PRIMARY KEY (symbol, unixtime, "interval");


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
