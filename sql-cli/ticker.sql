CREATE TABLE Ticker (
    symbol STRING,
    rowtime DATE,
    price INTEGER,
    tax INTEGER
) WITH ( 
    'connector' = 'filesystem',
    'path' = '/home/yl/proj/flink-test/sql-cli/ticker.csv',
    'format' = 'csv'
);

