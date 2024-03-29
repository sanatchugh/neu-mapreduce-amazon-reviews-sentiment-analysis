amazon = LOAD 's3://aox-pig/reviews_Amazon_Instant_Video.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (reviewerID,asin,reviewerName,helpful,reviewText,overall,summary,unixReviewTime,reviewTime);
reviewAmazon = FOREACH amazon GENERATE reviewerID, reviewText;
tokens = foreach reviewAmazon generate reviewerID,reviewText, FLATTEN(TOKENIZE(reviewText)) As word;
dictionary = load 's3://aox-pig/AFINN.txt' using PigStorage('\t') AS(word:chararray,rating:int);
word_rating = join tokens by word left outer, dictionary by word using 'replicated';
rating = foreach word_rating generate tokens::reviewerID as id, tokens::reviewText as text, dictionary::rating as rate;
word_group = group rating by (id,text);
avg_rate = foreach word_group generate group, AVG (rating.rate) as review_rating;
positive_review = filter avg_rate by review_rating>=0;
ordered_review = ORDER positive_review BY review_rating DESC;
top_ten_review = LIMIT ordered_review 10;
store top_ten_review into 's3://aox-pig/output_Amazon_Instant_Video_senti/';