CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER CHECK (score >= 0));
INSERT INTO users VALUES (1, 'ana', 10), (2, 'Bo', 20), (3, 'cy', NULL), (4, 'd''x', 40);

CREATE TABLE secret(id INTEGER PRIMARY KEY, password TEXT);
INSERT INTO secret VALUES (1, 'hunter2');

CREATE VIEW secret_view AS SELECT password FROM secret;

CREATE FUNCTION leak() RETURNS TEXT LANGUAGE sql AS $$ SELECT password FROM secret LIMIT 1 $$;

CREATE TABLE types(
    id INTEGER PRIMARY KEY,
    b BOOLEAN,
    i BIGINT,
    f DOUBLE PRECISION,
    n NUMERIC(10, 2),
    s TEXT,
    d DATE,
    ts TIMESTAMP,
    tz TIMESTAMPTZ,
    u UUID,
    bl BYTEA,
    l INTEGER[],
    j JSONB,
    e SERIAL
);
INSERT INTO types(id, b, i, f, n, s, d, ts, tz, u, bl, l, j) VALUES
    (1, true, 9007199254740993, 1.5, 12.34, 'x''y', '2024-02-29', '2024-02-29 12:34:56.789',
     '2024-02-29 12:34:56+00', '00000000-0000-0000-0000-000000000001', '\x00ff', '{1,2}', '{"k": [1, null]}'),
    (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
