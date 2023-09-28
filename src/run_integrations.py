import config.config as config
import integrations.kolada as k
import integrations.integrations as i
from database import database, crud, schemas
import multiprocessing as mp

"""
DEFAULT BATCH RUN
1. Import datablocks from each integration
2. Upsert datablocks to database
3. Insert timeseries for 1 geo
4. Calculate meta based on that -> upload to db

OPTIONALS 
1. Calculate avg 
"""

db_str = config.get('db_string')
kolada = k.KoladaIntegration()

db_session = database.Session()


# TODO: Don't hardcode geo_ids

def set_meta_for_block(block: schemas.DataBlock):
    if block.geo_groups == 'R':
        geo_id = '0001'  # Stockholm region
    else:
        geo_id = '0180'  # Stockholm municipality

    ts = kolada.get_timeseries(block, [geo_id])
    crud.insert_timeseries(db_session, ts)
    try:
        meta = crud.calculate_meta(db_session, block.data_id)
    except FileNotFoundError:
        print("data not found for id: " + block.source_id)
        return
    crud.update_datablock_meta(db_session, block.data_id, meta)


if __name__ == "__main__":
    crud.clear_timeseries_for_integration(db_session, kolada.integration_id)

    # Get data from integration
    datablock_list = kolada.get_datablocks()

    # Verify blocks with my super advanced algorithm
    if len(datablock_list) < 6000:
        exit("Where's the KPI's, Kolada!? Why you gotta do me like this? Thought we were friends...")

    # Insert the datablocks and do the normalisation magic
    crud.upsert_datablocks(db_session, datablock_list)

    # Calculate and set meta
    datablock_list = crud.get_datablocks(db_session, source='Kolada')
    print(len(datablock_list))

    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(set_meta_for_block, datablock_list)
    pool.close()
    pool.join()
