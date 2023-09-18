import db.write as db
import config.config as config
import integrations.kolada as k

db_str = config.get('db_string')
"""
SCHEDULE THIS IN BATCHES
1. Import datablocks from each integration
2. Upsert datablocks to database
"""

conn = db.Writer(db_str)
kolada = k.KoladaIntegration()

#Get data from integration
source_blocks = kolada.get_datablocks()

#Verify blocks with my super advanced algorithm
if len(source_blocks) < 6000:
    exit("Where's the KPI's, Kolada!? Why you gotta do me like this? Thought we were friends...")

#Insert the datablocks and do the normalisation magic
conn.upsert_datablocks(source_blocks)