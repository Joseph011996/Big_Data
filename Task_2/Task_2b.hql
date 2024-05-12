DROP TABLE twitterdata;
DROP TABLE totalTweet;


-- Create a table for the input data
CREATE TABLE twitterdata (tokenType STRING, month STRING, count BIGINT,
  hashtagName STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/twitter-small.tsv' INTO TABLE twitterdata;

-- TODO: *** Put your solution here ***
CREATE TABLE totalTweet AS
SELECT hashtagName, SUM(count) AS most
FROM twitterdata
GROUP BY hashtagName
Order By most DESC
LIMIT 1;


-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './Task_2b-out/'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  SELECT * FROM totalTweet;