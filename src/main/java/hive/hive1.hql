CREATE TABLE IF NOT EXISTS foodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/nicholas/Dropbox/Universita/BD-BigData/Project/1999_2006.tsv' 
OVERWRITE INTO TABLE foodreviews;

#1
CREATE VIEW average_score AS 
SELECT dg.ym, dg.productId, avg(dg.score) AS average 
FROM(
SELECT from_unixtime(time, 'Y/MM') AS ym, productId, score 
FROM foodreviews) dg 
GROUP BY dg.productId, dg.ym 
ORDER BY dg.ym, dg.productId;

#2
SELECT t.ym, t.productId, t.average 
FROM(
SELECT ym, productId, average, row_number() over(PARTITION BY ym ORDER BY average DESC) AS rank 
FROM average_score) t 
WHERE t.rank <= 5;
