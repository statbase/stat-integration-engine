default: run_integrations

setup_db:
	cd src && python3 -c "from database.scripts import scripts; scripts.create_db_schemas()" && cd ..

run_integrations: setup_db
	python3 src/run_integrations.py --meta

