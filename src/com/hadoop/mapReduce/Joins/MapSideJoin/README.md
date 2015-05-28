# Hadoop

Guys,

This gist demonstrates how to do a map-side join, loading one small dataset from DistributedCache into a HashMap in memory, and joining with a larger dataset.

Hadoop command to run this :

hadoop jar /home/cloudera/Downloads/MapJoin.jar com.hadoop.mapReduce.MapSideJoin.DriverMapSideJoinDCacheTxtFile /user/cloudera/Joins_Input/employees.txt /user/cloudera/Joins_Input/departments.txt /user/cloudera/Joins_OutPut


-----------------------------------------------------------------------------------------------------------------------------

You can reach me for any suggestions/clarifications on  : revanthkumar95@gmail.com                                              
Feel free to share any insights or constructive criticism. Cheers!!                                                           
#Happy hadooping!  
