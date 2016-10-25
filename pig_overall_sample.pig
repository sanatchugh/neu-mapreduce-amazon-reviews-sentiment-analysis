amazon = LOAD 's3://aox-pig/reviews_Amazon_Instant_Video.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (reviewerID,asin,reviewerName,helpful,reviewText,overall,summary,unixReviewTime,reviewTime);
reviewAmazon = FOREACH amazon GENERATE reviewerID, overall;
ordered_rating = ORDER reviewAmazon BY overall DESC;
top_ten_overall = LIMIT ordered_rating 10;
store top_ten_overall into 's3://aox-pig/output_Amazon_Instant_Video_overall/';