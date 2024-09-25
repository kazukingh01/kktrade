db.getSiblingDB("trade").getCollection("master_symbol").drop();
db.getSiblingDB("trade").createCollection("master_symbol");
db.getSiblingDB("trade").getCollection("master_symbol").createIndex({ symbol_id: 1 }, { unique: true });
db.getSiblingDB("trade").getCollection("master_symbol").createIndex({ exchange: 1, symbol_name: 1 }, { unique: true });
