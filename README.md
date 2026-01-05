# crea e fai girare il container
docker-compose build
docker-compose up -d
# esegui spark
docker-compose run --rm spark /opt/spark/bin/spark-submit /opt/spark-app/test_parquet.py

# ricrea container
docker-compose down
docker-compose up -d postgres

docker-compose run --rm spark /opt/spark/bin/spark-submit /opt/spark-app/ingest_conti.py