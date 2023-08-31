import db.db as db
import integrations.integrations as integrations

"""
SCHEDULE THIS IN BATCHES
1. Import datablocks from each integration
2. Upsert datablocks to database
3. Fetch normalised datablocks from database
4. Upsert timeseries to database
"""

conn = db.db_conn("db/stat-db.db")
kolada = integrations.KoladaIntegration()

#Get data from integration
source_blocks = kolada.get_datablocks()

#Verify blocks
if len(source_blocks) < 6000:
    exit("Where's the KPI's, Kolada!? Why you gotta do me like this?")

#Clear data table
conn.delete_datablocks_for_integration(kolada.integration_id)

#Insert the datablocks
conn.upsert_datablocks(source_blocks)

#Retrieve the datablocks as normalised
normalised_blocks = conn.get_datablocks_by_source("Kolada")

#Fetch the data from source using the normalised blocks
df = kolada.get_timeseries(normalised_blocks)

#Upsert the datablocks
conn.insert_timeseries(df)

#get timeseries from db
res = conn.get_timeseries_by_id(normalised_blocks[0])
print(res)