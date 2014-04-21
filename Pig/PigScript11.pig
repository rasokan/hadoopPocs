user_item = LOAD '/home/ashok/opt/hadoop/data/file/u.user' using PigStorage ('|') AS(user_id:int,age:int,gender:chararray,profile:chararray,zipcode:long);

filter_user = filter user_item by (gender == 'M' and profile == 'student');

rating_item = LOAD '/home/ashok/opt/hadoop/data/file/u.data' using PigStorage ('\t') as (user_id:int, movie_id:int, rating:int, timestamp:long);

selectRatingValue =  foreach rating_item generate movie_id,user_id;

X = join filter_user by filter_user.user_id, rating_item  by rating_item.user_id;

dump X;
