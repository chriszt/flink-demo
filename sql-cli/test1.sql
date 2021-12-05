CREATE TABLE test1 (
    id INT,
    name STRING,
    age INT
) WITH (
  'connector'='filesystem',
  'path'='/home/yl/proj/flink-demo/sql-cli/demo.csv',
  'format'='csv'
);
