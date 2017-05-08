CREATE TABLE IF NOT EXISTS foodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/nicholas/Dropbox/Universita/BD-BigData/Project/1999_2012.tsv' 
OVERWRITE INTO TABLE foodreviews;

CREATE VIEW similaruser as
SELECT a.userid as first, b.userid as second, COUNT(DISTINCT a.productid) as common_interests,
 collect_set(a.productId) as elencoProdottiComuni
FROM foodreviews a
JOIN foodreviews b on a.productId=b.productId
WHERE a.score > 3 and b.score > 3 and a.userid > b.userid
GROUP BY a.userid,b.userid;

SELECT sq.first, sq.second, sq.common_interests,sq.elencoProdottiComuni
FROM similaruser sq
WHERE sq.common_interests > 2
ORDER BY sq.first,sq.second;