create table userTable(
    id INTEGER,
    name STRING,
    age INTEGER
--     inTime TIMESTAMP default 0
) with (
    'connector' = 'filesystem',
    'path' = '/home/yl/proj/flink-demo/sql-cli/userTab.csv',
    'format' = 'csv'
);