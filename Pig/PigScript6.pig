register /home/ashok/Desktop/SingleMovieName.jar;

define singlenamedmovie com.learn.pig.udf.singlenamedmovie.SingleNamedMovie();

movie_item = LOAD '/home/ashok/opt/hadoop/data/file/u.item' using PigStorage ('|') AS (movie_id:int, movie_Name:chararray, release_date:chararray, empty:chararray, imdb_url:chararray, unknown:int, Action:int, Adventure:int, Animation:int, Children:int, Comedy:int, Crime:int, Documentary:int, Drama:int, Fantasy:int, Film_Noir:int, Horror:int, Musical:int, Mystery:int, Romance:int, Sci_Fi:int, Thriller:int, War:int, Western:int);

filter_Movie_Name = filter movie_item  by singlenamedmovie(movie_Name) and Romance == 1;

dump filter_Movie_Name;
