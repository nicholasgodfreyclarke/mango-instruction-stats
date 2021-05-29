BEGIN TRANSACTION;
DROP TABLE IF EXISTS "vault_flows";
CREATE TABLE IF NOT EXISTS "vault_flows" (
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
DROP TABLE IF EXISTS "oracle_transactions";
CREATE TABLE IF NOT EXISTS "oracle_transactions" (
	"signature"	TEXT,
	"slot"	INTEGER,
	"oracle_pk"	TEXT,
	"round_id"	INTEGER,
	"value"	INTEGER,
	"symbol"	TEXT
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
COMMIT;
