CREATE TABLE dukascopy_ohlcv (
    symbol SMALLINT NOT NULL,
    unixtime BIGINT NOT NULL,
    `interval` SMALLINT NOT NULL,
    price_open FLOAT,
    price_high FLOAT,
    price_low FLOAT,
    price_close FLOAT,
    ask_volume FLOAT,
    bid_volume FLOAT,
    PRIMARY KEY (symbol, unixtime, `interval`)
);

CREATE TABLE dukascopy_ticks (
    symbol SMALLINT NOT NULL,
    unixtime BIGINT NOT NULL,
    bid FLOAT,
    ask FLOAT,
    bid_size FLOAT,
    ask_size FLOAT
);

CREATE INDEX dukascopy_ohlcv_0 ON dukascopy_ohlcv (symbol);
CREATE INDEX dukascopy_ohlcv_1 ON dukascopy_ohlcv (unixtime);

CREATE INDEX dukascopy_ticks_0 ON dukascopy_ticks (symbol);
CREATE INDEX dukascopy_ticks_1 ON dukascopy_ticks (unixtime);
