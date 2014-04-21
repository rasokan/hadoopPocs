user_item = LOAD '/home/ashok/opt/hadoop/data/file/u.user' using PigStorage ('|') AS(user_id:int,age:int,gender:chararray,profile:chararray,zipcode:long);

group_user_By_Gender = group user_item.user_id by gender;

count_Male_Female = FOREACH group_user_By_Gender generate group, COUNT(user_item);


