import sys
import functools
import multiprocessing as mp
from integrations import kolada as k, integrations as i
from database import database, crud, schemas


"""
Default batch run
1. Import datablocks from each integration
2. Upsert datablocks to database
If --meta flag 
3. Insert timeseries for 1 geo
4. Calculate meta based on that -> upload to db

OPTIONALS 
1. Calculate avg 
"""
db_session = database.Session()


# TODO: Don't hardcode geo_ids

def set_meta_for_block(integration: i.BaseIntegration, block: schemas.DataBlock):
    if block.geo_groups == 'R':
        geo_id = '0001'  # Stockholm region
    else:
        geo_id = '0180'  # Stockholm municipality

    ts = integration.get_timeseries(block, [geo_id])
    crud.insert_timeseries(db_session, ts)
    try:
        meta = crud.calculate_meta(db_session, block.data_id)
    except FileNotFoundError:
        print("data not found for id: " + block.source_id)
        return
    crud.update_datablock_meta(db_session, block.data_id, meta)


if __name__ == "__main__":
    integrations = [k.KoladaIntegration()]

    for integration in integrations:
        crud.clear_timeseries_for_integration(db_session, integration.integration_id)

        # Get data from integration
        datablock_list = integration.get_datablocks()

        if len(datablock_list) < 6000:
            exit("Got no KPIs")

        # Insert the datablocks and do the normalisation magic
        crud.upsert_datablocks(db_session, datablock_list)


        if "--meta" in sys.argv:
            datablock_list = crud.get_datablocks(db_session)
            with mp.Pool(processes=mp.cpu_count()) as pool: # Basic pooling, may be improved 
                pool.map(functools.partial(set_meta_for_block, integration), datablock_list)
                pool.close()
                pool.join()
