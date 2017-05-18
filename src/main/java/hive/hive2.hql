CREATE TABLE IF NOT EXISTS foodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/nicholas/Dropbox/Universita/BD-BigData/Project/1999_2012.tsv' 
OVERWRITE INTO TABLE foodreviews;

SELECT t.userid, t.productId, t.score 
FROM(
SELECT userid, productId, score, row_number() over(PARTITION BY userid ORDER BY score DESC) as rank 
FROM foodreviews) t 
WHERE t.rank <= 10;