default: run_integrations

setup_db:
	cd src && python -c "from database.scripts import scripts; scripts.create_db_schemas()" && cd ..

run_integrations: setup_db
	python src/run_integrations.py --meta

