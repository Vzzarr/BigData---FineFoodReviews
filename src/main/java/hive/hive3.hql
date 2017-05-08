CREATE TABLE IF NOT EXISTS foodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/nicholas/Dropbox/Universita/BD-BigData/Project/1999_2006.tsv' 
OVERWRITE INTO TABLE foodreviews;

CREATE VIEW score3 
AS SELECT *
FROM foodreviews
WHERE score > 3;


CREATE VIEW product_user
AS SELECT DISTINCT t1.userid as first, t2.userid as second, t1.productid
FROM score3 as t1 INNER JOIN score3 as t2
WHERE t1.productid = t2.productid AND t1.userid > t2.userid;


SELECT relateds1.first, relateds1.second, relateds1.common_interests, relateds2.productid
FROM(
SELECT first, second, common_interests
FROM(
SELECT first, second, COUNT(*) as common_interests
FROM product_user
GROUP BY first, second) as t
WHERE t.common_interests > 2 AND t.second!=first) as relateds1 
JOIN 
product_user as relateds2
WHERE relateds1.first = relateds2.first AND relateds1.second = relateds2.second
ORDER BY relateds1.first, relateds1.second, relateds2.productid;
