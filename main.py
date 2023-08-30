import integrations.integrations as integrations
import db.db as db

def main():
    conn = db.db_conn("db/stat-db.db")
    kolada = integrations.KoladaIntegration()

    #Get data from integration
    source_blocks = kolada.get_datablocks()

    #Insert the data
    conn.upsert_datablocks(source_blocks)
    
    #Retrieve the data (with source_id)
    normalised_blocks = conn.get_datablocks_by_source("Kolada")
    #Fetch the data from source using the normalised blocks
    df = kolada.get_timeseries(normalised_blocks)
    print(df)
    


if __name__ == "__main__":
    main()


