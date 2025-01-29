db.getSiblingDB("trade").createCollection("mart_ohlc", { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})
