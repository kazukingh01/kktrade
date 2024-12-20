CREATE TABLE master_symbol (
    symbol_id SMALLINT NOT NULL,
    symbol_name VARCHAR(30) NOT NULL,
    exchange VARCHAR(30) NOT NULL,
    base VARCHAR(30) NOT NULL,
    currency VARCHAR(30) NOT NULL,
    is_active BOOLEAN NOT NULL,
    `explain` TEXT, -- `explain`is reserved word in MySQL.
    sys_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- 
-- Primary Key constraint
--

ALTER TABLE master_symbol
    ADD CONSTRAINT master_symbol_pkey PRIMARY KEY (symbol_id);

-- 
-- Unique constraint
--

ALTER TABLE master_symbol
    ADD CONSTRAINT master_symbol_unique_0 UNIQUE (exchange, symbol_name);
