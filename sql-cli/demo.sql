CREATE TABLE Demo (
    id INTEGER,
    name STRING,
    age INTEGER
) WITH ( 
    'connector' = 'filesystem',
    'path' = '/home/yl/proj/flink-test/sql-cli/demo.csv',
    'format' = 'csv'
);

