CREATE TABLE people (
    id INT,
    name VARCHAR,
    age INT
);

INSERT INTO people (id, name, age) VALUES (1, 'Emily', 25), (2, 'Alice', 30), (3, 'Lily', 35);

COPY people TO 'people.arrow' STORED AS arrow;
COPY people TO 'people.parquet' STORED AS parquet;
