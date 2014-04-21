user_item = LOAD '/home/ashok/opt/hadoop/data/file/u.user' using PigStorage ('|') AS(user_id:int,age:int,gender:chararray,profile:chararray,zipcode:long);

group_user_By_occupation = group user_item.user_id by profile;

count_user_By_occupation = FOREACH group_user_By_Gender generate group, COUNT(user_item);
