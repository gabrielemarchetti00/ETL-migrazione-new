CREATE TABLE etl_job (
    job_name TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE etl_run (
    run_id SERIAL PRIMARY KEY,
    job_name TEXT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    records_extracted INT,
    status TEXT
);

CREATE TABLE popso_conti (
    id_conto INT PRIMARY KEY,
    iban TEXT,
    saldo NUMERIC(15,2),
    cod_filiale TEXT,
    last_update TIMESTAMP
);

INSERT INTO popso_conti VALUES
(1, 'IT00TEST0001', 100.00, '001', NOW() - INTERVAL '2 days'),
(2, 'IT00TEST0002', 200.50, '002', NOW() - INTERVAL '1 days'),
(3, 'IT00TEST0003', -50.00, '003', NOW());