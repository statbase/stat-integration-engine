# stat-integration-engine

A tool that fetches and normalises statistical data from public sources.
https://excalidraw.com/#json=ZOUbDgS_wxDpZi7D3SpsE,4jQWhDwvkYDzGqceeLOecQ

### Run in Docker
See Dockerfile. Example: 
`docker build -t app . && docker run app`

### Run locally
1. Install dependencies: `pip install -r requirements.txt`
2. - Create a .db file in desired location. This will be the databased used to store any statistical entries.
3. Set variables in src/config/config.env (Make sure DB_PATH is absolute path)
   - Export them. Bash example: `export $(cat src/config/config.env | xargs)`
4. Setup & populate database: `make fetch_db` 
5. Start the server: `cd src && uvicorn main:app`

### Run linter
`pip install pylint`

`pylint $(find . -name "*.py")` 
