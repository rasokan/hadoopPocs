movie_item = LOAD '/home/ashok/opt/hadoop/data/file/u.item' using PigStorage ('|') AS (movie_id:int, movie_Name:chararray, release_date:chararray, empty:chararray, imdb_url:chararray, unknown:int, Action:int, Adventure:int, Animation:int, Children:int, Comedy:int, Crime:int, Documentary:int, Drama:int, Fantasy:int, Film_Noir:int, Horror:int, Musical:int, Mystery:int, Romance:int, Sci_Fi:int, Thriller:int, War:int, Western:int);

selectMovieName_GenreWar = foreach movie_item generate movie_Name,War;

filterMovieInfo_GenreInfo = filter selectMovieName_GenreWar by War == 1;

dump filterMovieInfo_GenreInfo;
