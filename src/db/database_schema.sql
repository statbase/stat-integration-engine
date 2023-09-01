--
-- File generated with SQLiteStudio v3.4.4 on Fri Sep 1 11:52:47 2023
--
-- Text encoding used: UTF-8
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- Table: category
CREATE TABLE IF NOT EXISTS "category" (
	"id"	INTEGER,
	"value"	TEXT NOT NULL,
	"parent_id"	INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT)
);

-- Table: data_block
CREATE TABLE IF NOT EXISTS "data_block" (
	"id"	INTEGER UNIQUE,
	"type"	TEXT NOT NULL,
	"meta"	TEXT,
	"source"	TEXT NOT NULL,
	"source_id"	TEXT UNIQUE,
	"category"	TEXT,
	"geo_groups"	TEXT,
	"title"	TEXT,
	"integration_id"	INTEGER,
	"var_labels"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT)
);

-- Table: entity
CREATE TABLE IF NOT EXISTS "entity" (
	"id"	INTEGER,
	"name"	INTEGER NOT NULL,
	"geo_id"	INTEGER NOT NULL,
	PRIMARY KEY("id" AUTOINCREMENT),
	FOREIGN KEY("geo_id") REFERENCES "geo_unit"("geo_id") ON DELETE CASCADE
);

-- Table: geo_unit
CREATE TABLE IF NOT EXISTS "geo_unit" (
	"type"	TEXT NOT NULL,
	"geo_id" TEXT NOT NULL,
	"name"	TEXT,
	PRIMARY KEY("geo_id")
);

-- Table: tag
CREATE TABLE IF NOT EXISTS "tag" (
	"id"	INTEGER,
	"value"	TEXT NOT NULL UNIQUE,
	PRIMARY KEY("id" AUTOINCREMENT)
);

-- Table: timeseries
CREATE TABLE IF NOT EXISTS "timeseries" (
	"ts_id"	INTEGER,
	"data_id"	INTEGER NOT NULL,
	"value"	NUMERIC NOT NULL,
	"date"	TEXT NOT NULL,
	"variable"	TEXT NOT NULL,
	"geo_id"	TEXT NOT NULL,
	PRIMARY KEY("ts_id" AUTOINCREMENT),
	UNIQUE("date","variable","geo_id","data_id"),
	FOREIGN KEY("data_id") REFERENCES "data_block"("id") ON DELETE CASCADE,
	FOREIGN KEY("geo_id") REFERENCES "geo_unit" ON DELETE CASCADE
);

-- Index: idx_timeseries_data_id
CREATE INDEX IF NOT EXISTS "idx_timeseries_data_id" ON "timeseries" (
	"data_id"	ASC
);

-- Index: idx_timeseries_data_id_geo_id
CREATE INDEX IF NOT EXISTS "idx_timeseries_data_id_geo_id" ON "timeseries" (
	"data_id",
	"geo_id"
);

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;
