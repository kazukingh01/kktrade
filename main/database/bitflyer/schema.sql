--
-- Name: bitflyer_executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_executions (
    symbol smallint NOT NULL,
    id bigint NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price real,
    size real
);


ALTER TABLE public.bitflyer_executions OWNER TO postgres;

--
-- Name: bitflyer_orderbook; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_orderbook (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    side smallint NOT NULL,
    price real,
    size real
);


ALTER TABLE public.bitflyer_orderbook OWNER TO postgres;

--
-- Name: bitflyer_ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_ticker (
    symbol smallint NOT NULL,
    tick_id bigint NOT NULL,
    state smallint,
    unixtime bigint,
    bid real,
    ask real,
    bid_size real,
    ask_size real,
    total_bid_depth real,
    total_ask_depth real,
    market_bid_size real,
    market_ask_size real,
    last_traded_price real,
    volume real,
    volume_by_product real
);


ALTER TABLE public.bitflyer_ticker OWNER TO postgres;

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
