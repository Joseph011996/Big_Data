DROP TABLE bankdata;
DROP TABLE filteredClients;

-- Create a table for the input data
CREATE TABLE bankdata (age BIGINT, job STRING, marital STRING, education STRING,
  default STRING, balance BIGINT, housing STRING, loan STRING, contact STRING,
  day BIGINT, month STRING, duration BIGINT, campaign BIGINT, pdays BIGINT,
  previous BIGINT, poutcome STRING, termdeposit STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank-small.csv' INTO TABLE bankdata;

-- TODO: *** Put your solution here ***
CREATE TABLE filteredClients AS
SELECT marital, COUNT(1) AS count
FROM bankdata
WHERE loan LIKE "yes" AND balance >  500
GROUP BY marital;
 
-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './Task_1a-out/'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  SELECT * FROM filteredClients;
