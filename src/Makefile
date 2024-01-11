default: fetch_db

setup_db:
	python3 -c "from database.scripts import scripts; scripts.create_db_schemas()"

fetch_db: setup_db
	python3 run_integrations.py 

fetch_db_meta: setup_db
	python3 run_integrations.py --meta
