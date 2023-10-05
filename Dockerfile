FROM python:3.10

ENV DB_PATH=stat-db.py

WORKDIR app

COPY src/ app/src/
COPY requirements.txt /app/

EXPOSE 8080

RUN ["pip", "install", "-r", "requirements.txt"]

WORKDIR app/src

RUN ["python", "-c", "from database.scripts import scripts; scripts.create_db_schemas()"]

RUN ["python", "run_integrations.py", "--meta"]

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]