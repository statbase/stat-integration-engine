-- The first migration to start the database.

CREATE TABLE "category" (
	"id"	INTEGER,
	"value"	TEXT NOT NULL,
	"parent_id"	INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE "data_block" (
	"id"	INTEGER UNIQUE,
	"type"	TEXT NOT NULL,
	"meta"	TEXT,
	"source"	TEXT NOT NULL,
	"source_id"	TEXT UNIQUE,
	"category"	TEXT,
	"geo_groups"	TEXT,
	"title"	TEXT,
	"integration_id"	INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE "entity" (
	"id"	INTEGER,
	"name"	INTEGER NOT NULL,
	"geo_id"	INTEGER NOT NULL,
	PRIMARY KEY("id" AUTOINCREMENT),
	FOREIGN KEY("geo_id") REFERENCES "geo_unit"("geo_id") ON DELETE CASCADE
);
CREATE TABLE "geo_unit" (
	"type"	TEXT NOT NULL,
	"geo_id" TEXT NOT NULL,
	"name"	TEXT,
	PRIMARY KEY("geo_id")
);
CREATE TABLE "tag" (
	"id"	INTEGER,
	"value"	TEXT NOT NULL UNIQUE,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE "timeseries" (
	"ts_id"	INTEGER,
	"data_id"	INTEGER NOT NULL,
	"value"	NUMERIC NOT NULL,
	"date"	TEXT NOT NULL,
	"variable"	TEXT NOT NULL,
	"geo_id"	TEXT NOT NULL,
	FOREIGN KEY("geo_id") REFERENCES "geo_unit" ON DELETE CASCADE,
	FOREIGN KEY("data_id") REFERENCES "data_block"("ts_id") ON DELETE CASCADE,
	PRIMARY KEY("ts_id" AUTOINCREMENT),
	UNIQUE("date","variable","geo_id","data_id")
);
CREATE INDEX "idx_timeseries_data_id" ON "timeseries" (
	"data_id"	ASC
);
CREATE INDEX "idx_timeseries_data_id_geo_id" ON "timeseries" (
	"data_id",
	"geo_id"
);

