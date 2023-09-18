import sqlite3
import re
import sys
import os
sys.path.append(os.getcwd()) 
import config.config as config

communes = {}
regions = {}

create_table = """
CREATE TABLE "geo_unit" (
	"type"	TEXT NOT NULL,
	"geo_id" TEXT NOT NULL,
	"name"	TEXT,
	PRIMARY KEY("geo_id")
)
"""
delete_table = 'DROP TABLE IF EXISTS geo_unit'

#Reset schema
conn = sqlite3.connect(config.get('db_string'))
cur = conn.cursor()
cur.execute(delete_table)
cur.execute(create_table)

# Process CSV
with open('db/scripts/geo_data.csv', 'r') as file:
    for line in file:
        key, value = line.strip().split(';')
        if " län" in value:
            regions[key] = re.sub(r'\s+', ' ', value)
        else:
            communes[key] = value

#Upload communes
for id, name in communes.items():
    cur.execute("""INSERT INTO geo_unit (geo_id, type, name) 
                    VALUES (?, ?, ?)""",
                    (id,"commune", name))

#Upload regions
for id, name in regions.items():
    cur.execute("""INSERT INTO geo_unit (geo_id, type, name) 
                    VALUES (?, ?, ?)""",
                    (id,"region", name))
    
conn.commit()
conn.close()

print("let's hope everything worked :)")