CREATE TABLE mart_ohlc (
    symbol SMALLINT NOT NULL,
    unixtime DATETIME NOT NULL,
    type SMALLINT NOT NULL,
    `interval` INT NOT NULL,
    sampling_rate INT NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    ave FLOAT,
    attrs JSON,
    PRIMARY KEY (symbol, unixtime, type, `interval`, sampling_rate)
);

CREATE INDEX mart_ohlc_0 ON mart_ohlc (symbol);
CREATE INDEX mart_ohlc_1 ON mart_ohlc (unixtime);
CREATE INDEX mart_ohlc_2 ON mart_ohlc (type);
CREATE INDEX mart_ohlc_3 ON mart_ohlc (`interval`);
CREATE INDEX mart_ohlc_4 ON mart_ohlc (sampling_rate);