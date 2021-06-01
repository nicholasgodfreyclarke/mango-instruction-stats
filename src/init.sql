BEGIN TRANSACTION;
DROP TABLE IF EXISTS "liquidation_holdings";
CREATE TABLE IF NOT EXISTS "liquidation_holdings" (
	"signature"	TEXT,
	"symbol"	TEXT,
	"assets"	NUMERIC,
	"liabs"	NUMERIC,
	"price"	NUMERIC
);
DROP TABLE IF EXISTS "liquidations";
CREATE TABLE IF NOT EXISTS "liquidations" (
	"signature"	TEXT,
	"liqor"	TEXT,
	"liquee"	TEXT,
	"coll_ratio"	NUMERIC,
	"in_token_symbol"	TEXT,
	"in_token_amount"	NUMERIC,
	"in_token_price"	NUMERIC,
	"out_token_symbol"	TEXT,
	"out_token_amount"	NUMERIC,
	"out_token_price"	NUMERIC
);
DROP TABLE IF EXISTS "transactions";
CREATE TABLE IF NOT EXISTS "transactions" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
	"signature"	TEXT,
	"account"	TEXT,
	"block_time"	INTEGER,
	"block_datetime"	TEXT,
	"slot"	INTEGER,
	"err"	INTEGER,
	"process_state"	TEXT,
	"log_messages"	TEXT,
	"compute"	INTEGER
);
DROP TABLE IF EXISTS "oracle_transactions";
CREATE TABLE IF NOT EXISTS "oracle_transactions" (
	"signature"	TEXT,
	"slot"	INTEGER,
	"oracle_pk"	TEXT,
	"round_id"	INTEGER,
	"value"	INTEGER,
	"symbol"	TEXT
);
DROP TABLE IF EXISTS "deposit_withdraw";
CREATE TABLE IF NOT EXISTS "deposit_withdraw" (
	"signature"	TEXT,
	"instruction_num"	INTEGER,
	"slot"	INTEGER,
	"owner"	TEXT,
	"side"	TEXT,
	"quantity"	NUMERIC,
	"symbol"	TEXT,
	"symbol_price"	NUMERIC,
	"usd_equivalent"	NUMERIC
);
COMMIT;
