item = LOAD '/home/ashok/opt/hadoop/data/file/u.item' using PigStorage ('|') AS (movie_id:int, movie_Name:chararray, release_date:chararray, empty:chararray, imdb_url:chararray, unknown:int, Action:int, Adventure:int, Animation:int, Children:int, Comedy:int, Crime:int, Documentary:int, Drama:int, Fantasy:int, Film_Noir:int, Horror:int, Musical:int, Mystery:int, Romance:int, Sci_Fi:int, Thriller:int, War:int, Western:int);

selectDateMovieName = foreach item generate release_date,movie_Name;

selectYearMovieName = foreach selectDateMovieName generate TRIM(SUBSTRING(release_date, 7, 11)) as year,release_date,movie_name;

filterYear = filter selectYearMovieName by (year matches '199.*') or (year == '2000');

store filterYear into '/home/ashok/opt/hadoop/data/file/releasebyyear1';

Dump filterYear;
