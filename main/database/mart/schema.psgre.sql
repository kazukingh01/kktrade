CREATE TABLE public.mart_ohlc (
    symbol smallint NOT NULL,
    unixtime timestamp with time zone NOT NULL,
    type smallint NOT NULL,
    "interval" integer NOT NULL,
    open real,
    high real,
    low real,
    close real,
    ave real,
    attrs jsonb
);

ALTER TABLE ONLY public.mart_ohlc
    ADD CONSTRAINT mart_ohlc_pkey PRIMARY KEY (symbol, unixtime, type, "interval");

CREATE INDEX mart_ohlc_0 ON public.mart_ohlc USING btree (symbol);
CREATE INDEX mart_ohlc_1 ON public.mart_ohlc USING btree (unixtime);
CREATE INDEX mart_ohlc_2 ON public.mart_ohlc USING btree (type);
CREATE INDEX mart_ohlc_3 ON public.mart_ohlc USING btree ("interval");
