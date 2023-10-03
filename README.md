# stat-integration-engine

A tool that fetches and normalises statistical data from public sources.

[![image](https://github.com/statbase/stat-integration-engine/assets/38020265/679d90a5-9193-4738-b2f2-056a089815c6)](https://excalidraw.com/#json=ZOUbDgS_wxDpZi7D3SpsE,4jQWhDwvkYDzGqceeLOecQ)https://excalidraw.com/#json=ZOUbDgS_wxDpZi7D3SpsE,4jQWhDwvkYDzGqceeLOecQ

### Run in Docker
See Dockerfile. Example: 
`docker build -t app . && docker run app`

### Run locally
1. Install dependencies: `pip install -r requirements.txt`
2. Set variables in src/config/config.env (Make sure DB_STRING is absolute path)
   - Export them. Bash example: `export $(cat src/config/config.env | xargs)`
3. Setup & populate database: `make run_integrations`
4. Start the server: `cd src && uvicorn main:app`

### Run linter
`pip install pylint`

`pylint $(find . -name "*.py")` 
