CREATE TABLE economic_calendar (
    id INT NOT NULL,
    unixtime BIGINT NOT NULL,
    name VARCHAR(255),
    country VARCHAR(255),
    importance SMALLINT NOT NULL,
    actual FLOAT,
    previous FLOAT,
    consensus FLOAT,
    forecast FLOAT,
    unit VARCHAR(255),
    unit2 VARCHAR(255),
    PRIMARY KEY (id, unixtime)
);

CREATE INDEX economic_calendar_0 ON economic_calendar (id);
CREATE INDEX economic_calendar_1 ON economic_calendar (unixtime);
