# crea e fai girare il container
docker-compose build
docker-compose up -d
# esegui spark
docker-compose run --rm spark /opt/spark/bin/spark-submit /opt/spark-app/test_parquet.py

# ricrea container
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d postgres

# runna spark
docker-compose run --rm spark /opt/spark/bin/spark-submit --jars /opt/jars/postgresql.jar /opt/spark-app/ingest_conti.py

# vedi file dentro container
docker-compose run --rm spark bash

# entro in postgres
docker exec -it etl-postgres psql -U etl etl_metadata
\dt: vedo tabelle
\q: esci

# normalizzazione
docker-compose run --rm spark /opt/spark/bin/spark-submit --jars file:///opt/jars/postgresql.jar /opt/spark-app/normalize_conti.py

# esportazione su file csv
docker-compose run --rm spark /opt/spark/bin/spark-submit /opt/spark-app/load_conti_csv.py