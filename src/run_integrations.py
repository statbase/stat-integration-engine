import db.write as dbwrite, db.read as dbread
import config.config as config
import integrations.kolada as k
import multiprocessing as mp

"""
SCHEDULE THIS IN BATCHES
1. Import datablocks from each integration
2. Upsert datablocks to database
3. Insert timeseries for 1 geo
4. Calculate meta based on that -> upload to db
"""

db_str = config.get('db_string')
kolada = k.KoladaIntegration()

writer = dbwrite.Writer(db_str)
reader = dbread.Reader(db_str)


def set_meta_for_block(block):
    if block.geo_groups == 'R':
        geo_id = '0001'  # Stockholm region
    else:
        geo_id = '0180'  # Stockholm municipality
    ts = kolada.get_timeseries(block, [geo_id])
    writer.insert_timeseries(ts)
    try:
        meta = reader.calculate_meta(block.data_id)
    except FileNotFoundError:
        print("data not found for id: " + block.source_id)
        return
    writer.update_meta(block.data_id, meta)


if __name__ == "__main__":
    # Get data from integration
    datablock_list = kolada.get_datablocks()

    # Verify blocks with my super advanced algorithm
    if len(datablock_list) < 6000:
        exit("Where's the KPI's, Kolada!? Why you gotta do me like this? Thought we were friends...")

    # Insert the datablocks and do the normalisation magic
    writer.upsert_datablocks(datablock_list)

    # Calculate and set meta
    writer.clear_timeseries_for_integration(kolada.integration_id)
    datablock_list = reader.get_datablocks_by_filters(source='Kolada')
    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(set_meta_for_block, datablock_list)
    pool.close()
    pool.join()

    # Cleanup
    writer.close()
    reader.close()
