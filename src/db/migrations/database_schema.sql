PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- Table: data_block
CREATE TABLE IF NOT EXISTS data_block (data_id INTEGER UNIQUE, type TEXT NOT NULL, meta TEXT, source TEXT NOT NULL, source_id TEXT UNIQUE, tags TEXT, geo_groups TEXT, title TEXT, integration_id INTEGER, var_labels TEXT, description TEXT, meta TEXT, PRIMARY KEY (data_id AUTOINCREMENT));

-- Table: geo_unit
CREATE TABLE IF NOT EXISTS "geo_unit" (
	"type"	TEXT NOT NULL,
	"geo_id" TEXT NOT NULL,
	"name"	TEXT,
	PRIMARY KEY("geo_id")
);

-- Table: timeseries
CREATE TABLE IF NOT EXISTS timeseries (ts_id INTEGER, 
data_id INTEGER NOT NULL, 
value NUMERIC NOT NULL, 
date TEXT NOT NULL, 
variable TEXT NOT NULL, 
geo_id TEXT NOT NULL, 
PRIMARY KEY (ts_id AUTOINCREMENT), 
UNIQUE (date, variable, geo_id, data_id), 
FOREIGN KEY (data_id) REFERENCES data_block (data_id) ON DELETE CASCADE, 
FOREIGN KEY (geo_id) REFERENCES geo_unit ON DELETE CASCADE);

-- Index: idx_timeseries_data_id
CREATE INDEX IF NOT EXISTS idx_timeseries_data_id ON timeseries ("data_id" ASC);

-- Index: idx_timeseries_data_id_geo_id
CREATE INDEX IF NOT EXISTS idx_timeseries_data_id_geo_id ON timeseries ("data_id", "geo_id");

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;