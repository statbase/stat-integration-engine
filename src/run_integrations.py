import db.write as dbwrite, db.read as dbread
import config.config as config
import integrations.kolada as k
import integrations.integrations as i
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

db_writer = dbwrite.Writer(db_str)
db_reader = dbread.Reader(db_str)


# TODO: Enum or some shit for the method instead
def calculate_national_mean(method: i.AggMethod, integration: i.BaseIntegration, reader: db_reader) -> float:
    pass


# TODO: Don't hardcode geo_ids
def set_meta_for_block(block):
    if block.geo_groups == 'R':
        geo_id = '0001'  # Stockholm region
    else:
        geo_id = '0180'  # Stockholm municipality

    ts = kolada.get_timeseries(block, [geo_id])
    db_writer.insert_timeseries(ts)
    try:
        meta = db_reader.calculate_meta(block.data_id)
    except FileNotFoundError:
        print("data not found for id: " + block.source_id)
        return
    db_writer.update_meta(block.data_id, meta)


if __name__ == "__main__":
    db_writer.clear_timeseries_for_integration(kolada.integration_id)
    # Get data from integration
    datablock_list = kolada.get_datablocks()

    # Verify blocks with my super advanced algorithm
    if len(datablock_list) < 6000:
        exit("Where's the KPI's, Kolada!? Why you gotta do me like this? Thought we were friends...")

    # Insert the datablocks and do the normalisation magic
    db_writer.upsert_datablocks(datablock_list)

    # Calculate and set meta
    """
    datablock_list = db_reader.get_datablocks_by_filters(source='Kolada')
    
    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(set_meta_for_block, datablock_list)
    pool.close()
    pool.join()
    """
    # Cleanup
    db_writer.close()
    db_reader.close()
