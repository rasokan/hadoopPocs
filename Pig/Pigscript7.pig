movie_item = LOAD '/home/ashok/opt/hadoop/data/file/u.item' using PigStorage ('|') AS (movie_id:int, movie_Name:chararray, release_date:chararray, empty:chararray, imdb_url:chararray, unknown:int, Action:int, Adventure:int, Animation:int, Children:int, Comedy:int, Crime:int, Documentary:int, Drama:int, Fantasy:int, Film_Noir:int, Horror:int, Musical:int, Mystery:int, Romance:int, Sci_Fi:int, Thriller:int, War:int, Western:int);

rating_item = LOAD '/home/ashok/opt/hadoop/data/file/u.data' using PigStorage ('\t') as (user_id:int, movie_id:int, rating:int, timestamp:long);

selectMovieId_Name = foreach movie_item generate movie_id,movie_Name;

selectRatingValue =  foreach rating_item generate movie_id,rating;

group_rating = group selectRatingValue by movie_id;

sum_rating = foreach group_rating generate group as movie_id, SUM(selectRatingValue.rating) as sums;

filter_sum_rating = filter sum_rating by $1>=5;

dump filter_sum_rating;
