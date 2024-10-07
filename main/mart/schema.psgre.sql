CREATE TABLE public.mart_ohlc (
    symbol smallint NOT NULL,
    unixtime timestamp with time zone NOT NULL,
    "interval" integer NOT NULL,
    open real,
    high real,
    low real,
    close real,
    ave real,
    ave_ask real,
    size_ask real,
    ntx_ask integer,
    amount_ask real,
    ave_bid real,
    size_bid real,
    ntx_bid integer,
    amount_bid real,
    attrs jsonb,
    PRIMARY KEY (symbol, unixtime, `interval`)
);

ALTER TABLE ONLY public.mart_ohlc
    ADD CONSTRAINT mart_ohlc_pkey PRIMARY KEY (symbol, unixtime, "interval");

CREATE INDEX mart_ohlc_0 ON public.mart_ohlc USING btree (symbol);
CREATE INDEX mart_ohlc_1 ON public.mart_ohlc USING btree (unixtime);
CREATE INDEX mart_ohlc_2 ON public.mart_ohlc USING btree ("interval");
