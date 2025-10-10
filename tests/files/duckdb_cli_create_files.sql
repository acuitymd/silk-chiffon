ATTACH 'people.duckdb' as people_duckdb;

CREATE TABLE people_duckdb.people (
    id INT,
    name VARCHAR,
    age INT
);

INSERT INTO people_duckdb.people (id, name, age) VALUES (1, 'Emily', 25);
INSERT INTO people_duckdb.people (id, name, age) VALUES (2, 'Alice', 30);
INSERT INTO people_duckdb.people (id, name, age) VALUES (3, 'Lily', 35);

DETACH people_duckdb;