FROM spark:3.5.0-scala2.12-java11-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install pyspark psycopg2-binary sqlalchemy && \
    rm -rf /var/lib/apt/lists/*

USER spark