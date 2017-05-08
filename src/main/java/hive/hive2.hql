CREATE TABLE IF NOT EXISTS foodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/nicholas/Dropbox/Universita/BD-BigData/Project/1999_2012.tsv' 
OVERWRITE INTO TABLE foodreviews;

CREATE VIEW average_score_user AS 
SELECT dg.userid, dg.productId, avg(dg.score) AS average 
FROM(
SELECT userid, productId, score 
FROM foodreviews) dg 
GROUP BY dg.productId, dg.userid 
ORDER BY dg.userid, dg.productId;

SELECT t.userid, t.productId, t.average 
FROM(
SELECT userid, productId, average, row_number() over(PARTITION BY userid ORDER BY average DESC) as rank 
FROM average_score_user) t 
WHERE t.rank <= 10;
