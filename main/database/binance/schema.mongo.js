db.getSiblingDB("trade").createCollection("binance_executions",    { timeseries: {timeField: "unixtime", metaField: "symbol", bucketMaxSpanSeconds: 360, bucketRoundingSeconds: 360 }})
db.getSiblingDB("trade").createCollection("binance_orderbook",     { timeseries: {timeField: "unixtime", metaField: "symbol", bucketMaxSpanSeconds: 360, bucketRoundingSeconds: 360 }})
db.getSiblingDB("trade").createCollection("binance_kline",         { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("binance_funding_rate",  { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("binance_open_interest", { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("binance_long_short",    { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("binance_taker_volume",  { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
