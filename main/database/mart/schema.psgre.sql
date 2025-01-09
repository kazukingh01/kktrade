CREATE TABLE public.mart_ohlc (
    symbol smallint NOT NULL,
    unixtime timestamp with time zone NOT NULL,
    type smallint NOT NULL,
    "interval" integer NOT NULL,
    sampling_rate integer NOT NULL,
    open real,
    high real,
    low real,
    close real,
    ave real,
    volume real,
    size real,
    attrs jsonb
) PARTITION BY RANGE (unixtime);


ALTER TABLE public.mart_ohlc OWNER TO postgres;


CREATE TABLE mart_ohlc_2019 PARTITION OF mart_ohlc FOR VALUES FROM ('2019-01-01 00:00:00+00') TO ('2020-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2020 PARTITION OF mart_ohlc FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2021-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2021 PARTITION OF mart_ohlc FOR VALUES FROM ('2021-01-01 00:00:00+00') TO ('2022-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2022 PARTITION OF mart_ohlc FOR VALUES FROM ('2022-01-01 00:00:00+00') TO ('2023-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2023 PARTITION OF mart_ohlc FOR VALUES FROM ('2023-01-01 00:00:00+00') TO ('2024-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2024 PARTITION OF mart_ohlc FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2025 PARTITION OF mart_ohlc FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2026 PARTITION OF mart_ohlc FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2027 PARTITION OF mart_ohlc FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2028-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2028 PARTITION OF mart_ohlc FOR VALUES FROM ('2028-01-01 00:00:00+00') TO ('2029-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2029 PARTITION OF mart_ohlc FOR VALUES FROM ('2029-01-01 00:00:00+00') TO ('2030-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2030 PARTITION OF mart_ohlc FOR VALUES FROM ('2030-01-01 00:00:00+00') TO ('2031-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2031 PARTITION OF mart_ohlc FOR VALUES FROM ('2031-01-01 00:00:00+00') TO ('2032-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2032 PARTITION OF mart_ohlc FOR VALUES FROM ('2032-01-01 00:00:00+00') TO ('2033-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2033 PARTITION OF mart_ohlc FOR VALUES FROM ('2033-01-01 00:00:00+00') TO ('2034-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2034 PARTITION OF mart_ohlc FOR VALUES FROM ('2034-01-01 00:00:00+00') TO ('2035-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2035 PARTITION OF mart_ohlc FOR VALUES FROM ('2035-01-01 00:00:00+00') TO ('2036-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2036 PARTITION OF mart_ohlc FOR VALUES FROM ('2036-01-01 00:00:00+00') TO ('2037-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2037 PARTITION OF mart_ohlc FOR VALUES FROM ('2037-01-01 00:00:00+00') TO ('2038-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2038 PARTITION OF mart_ohlc FOR VALUES FROM ('2038-01-01 00:00:00+00') TO ('2039-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2039 PARTITION OF mart_ohlc FOR VALUES FROM ('2039-01-01 00:00:00+00') TO ('2040-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2040 PARTITION OF mart_ohlc FOR VALUES FROM ('2040-01-01 00:00:00+00') TO ('2041-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2041 PARTITION OF mart_ohlc FOR VALUES FROM ('2041-01-01 00:00:00+00') TO ('2042-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2042 PARTITION OF mart_ohlc FOR VALUES FROM ('2042-01-01 00:00:00+00') TO ('2043-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2043 PARTITION OF mart_ohlc FOR VALUES FROM ('2043-01-01 00:00:00+00') TO ('2044-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2044 PARTITION OF mart_ohlc FOR VALUES FROM ('2044-01-01 00:00:00+00') TO ('2045-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2045 PARTITION OF mart_ohlc FOR VALUES FROM ('2045-01-01 00:00:00+00') TO ('2046-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2046 PARTITION OF mart_ohlc FOR VALUES FROM ('2046-01-01 00:00:00+00') TO ('2047-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2047 PARTITION OF mart_ohlc FOR VALUES FROM ('2047-01-01 00:00:00+00') TO ('2048-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2048 PARTITION OF mart_ohlc FOR VALUES FROM ('2048-01-01 00:00:00+00') TO ('2049-01-01 00:00:00+00');
CREATE TABLE mart_ohlc_2049 PARTITION OF mart_ohlc FOR VALUES FROM ('2049-01-01 00:00:00+00') TO ('2050-01-01 00:00:00+00');

ALTER TABLE ONLY public.mart_ohlc_2019 ADD CONSTRAINT mart_ohlc_2019_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2020 ADD CONSTRAINT mart_ohlc_2020_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2021 ADD CONSTRAINT mart_ohlc_2021_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2022 ADD CONSTRAINT mart_ohlc_2022_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2023 ADD CONSTRAINT mart_ohlc_2023_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2024 ADD CONSTRAINT mart_ohlc_2024_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2025 ADD CONSTRAINT mart_ohlc_2025_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2026 ADD CONSTRAINT mart_ohlc_2026_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2027 ADD CONSTRAINT mart_ohlc_2027_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2028 ADD CONSTRAINT mart_ohlc_2028_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2029 ADD CONSTRAINT mart_ohlc_2029_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2030 ADD CONSTRAINT mart_ohlc_2030_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2031 ADD CONSTRAINT mart_ohlc_2031_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2032 ADD CONSTRAINT mart_ohlc_2032_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2033 ADD CONSTRAINT mart_ohlc_2033_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2034 ADD CONSTRAINT mart_ohlc_2034_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2035 ADD CONSTRAINT mart_ohlc_2035_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2036 ADD CONSTRAINT mart_ohlc_2036_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2037 ADD CONSTRAINT mart_ohlc_2037_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2038 ADD CONSTRAINT mart_ohlc_2038_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2039 ADD CONSTRAINT mart_ohlc_2039_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2040 ADD CONSTRAINT mart_ohlc_2040_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2041 ADD CONSTRAINT mart_ohlc_2041_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2042 ADD CONSTRAINT mart_ohlc_2042_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2043 ADD CONSTRAINT mart_ohlc_2043_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2044 ADD CONSTRAINT mart_ohlc_2044_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2045 ADD CONSTRAINT mart_ohlc_2045_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2046 ADD CONSTRAINT mart_ohlc_2046_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2047 ADD CONSTRAINT mart_ohlc_2047_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2048 ADD CONSTRAINT mart_ohlc_2048_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);
ALTER TABLE ONLY public.mart_ohlc_2049 ADD CONSTRAINT mart_ohlc_2049_pkey PRIMARY KEY (symbol, unixtime, type, "interval", sampling_rate);

CREATE INDEX mart_ohlc_0 ON public.mart_ohlc USING btree (symbol);
CREATE INDEX mart_ohlc_1 ON public.mart_ohlc USING btree (unixtime);
CREATE INDEX mart_ohlc_2 ON public.mart_ohlc USING btree (symbol, unixtime);
-- CREATE INDEX mart_ohlc_2 ON public.mart_ohlc USING btree (type);
-- CREATE INDEX mart_ohlc_3 ON public.mart_ohlc USING btree ("interval");
-- CREATE INDEX mart_ohlc_4 ON public.mart_ohlc USING btree (sampling_rate);
