CREATE TABLE mart_ohlc (
    symbol SMALLINT NOT NULL,
    unixtime BIGINT NOT NULL,
    `interval` INT NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    ave FLOAT,
    ave_ask FLOAT,
    size_ask FLOAT,
    ntx_ask INT,
    amount_ask FLOAT,
    ave_bid FLOAT,
    size_bid FLOAT,
    ntx_bid INT,
    amount_bid FLOAT,
    attrs JSON,
    PRIMARY KEY (symbol, unixtime, `interval`)
);

CREATE INDEX mart_ohlc_0 ON mart_ohlc (symbol);
CREATE INDEX mart_ohlc_1 ON mart_ohlc (unixtime);
CREATE INDEX mart_ohlc_2 ON mart_ohlc (`interval`);