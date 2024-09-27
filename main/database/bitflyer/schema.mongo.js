db.getSiblingDB("trade").createCollection("bitflyer_executions",  { timeseries: {timeField: "unixtime", metaField: "symbol", bucketMaxSpanSeconds: 360, bucketRoundingSeconds: 360 }})
db.getSiblingDB("trade").createCollection("bitflyer_orderbook",   { timeseries: {timeField: "unixtime", metaField: "symbol", bucketMaxSpanSeconds: 360, bucketRoundingSeconds: 360 }})
db.getSiblingDB("trade").createCollection("bitflyer_fundingrate", { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("bitflyer_ticker",      { timeseries: {timeField: "unixtime", metaField: "symbol", granularity: "seconds" }})
