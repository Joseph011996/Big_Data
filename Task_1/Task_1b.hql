DROP TABLE bankdata;
DROP TABLE avgBalance;

-- Create a table for the input data
CREATE TABLE bankdata (age BIGINT, job STRING, marital STRING, education STRING,
  default STRING, balance BIGINT, housing STRING, loan STRING, contact STRING,
  day BIGINT, month STRING, duration BIGINT, campaign BIGINT, pdays BIGINT,
  previous BIGINT, poutcome STRING, termdeposit STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank-small.csv' INTO TABLE bankdata;

-- TODO: *** Put your solution here ***
CREATE TABLE avgBalance AS
SELECT job, AVG(balance) AS Average_Yearly
FROM bankdata
GROUP BY job
ORDER BY Average_Yearly DESC;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './Task_1b-out/'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  SELECT * FROM avgBalance;
