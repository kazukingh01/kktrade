CREATE TABLE master_symbol (
    symbol_id SMALLINT NOT NULL,
    symbol_name VARCHAR(30) NOT NULL,
    exchange VARCHAR(30) NOT NULL,
    base VARCHAR(30) NOT NULL,
    currency VARCHAR(30) NOT NULL,
    is_active BOOLEAN NOT NULL,
    `explain` TEXT, -- `explain` は MySQL で予約語なのでバックティックで囲みます
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

-- 
-- Trigger equivalent in MySQL
--

DELIMITER $$

CREATE TRIGGER trg_update_sys_updated_master_symbol 
BEFORE UPDATE ON master_symbol 
FOR EACH ROW 
BEGIN
    SET NEW.sys_updated = CURRENT_TIMESTAMP;
END$$

DELIMITER ;

