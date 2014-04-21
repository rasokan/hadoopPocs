movie_item = LOAD '/home/ashok/opt/hadoop/data/file/u.item' using PigStorage ('|') AS (movie_id:int, movie_Name:chararray, release_date:chararray, empty:chararray, imdb_url:chararray, unknown:int, Action:int, Adventure:int, Animation:int, Children:int, Comedy:int, Crime:int, Documentary:int, Drama:int, Fantasy:int, Film_Noir:int, Horror:int, Musical:int, Mystery:int, Romance:int, Sci_Fi:int, Thriller:int, War:int, Western:int);

selectMovieId_Name = foreach movie_item generate movie_id,movie_Name,unknown+Action+Adventure+Animation+Children+Comedy+Crime+Documentary+Drama+Fantasy+Film_Noir+Horror+Musical+Mystery+Romance+Sci_Fi+Thriller+War+Western as count_Genre;

count_genre_Movie_Name = foreach selectMovieId_Name generate $0,$1,$2;

filter_count_genre_Movie_Name =  filter count_genre_Movie_Name by $3 >=2;

group_count_filter_count_genre_movie_Name = group filter_count_genre_Movie_Name by all;

total_count_filter_count_genre_movie_Name = foreach group_count_filter_count_genre_movie_Name generate count($1);

dump total_count_filter_count_genre_movie_Name;
