CREATE EXTERNAL TABLE transactions
(
    txnno INT,
    txndate STRING,
    custno INT,
    amount DOUBLE,
    product STRING,
    category STRING,
    city STRING,
    state STRING,
    payment_mode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/splab298046/datasets/transactions';
