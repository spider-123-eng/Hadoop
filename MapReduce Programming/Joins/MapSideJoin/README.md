# Hadoop

Guys,

This gist demonstrates how to do a map-side join, loading one small dataset from DistributedCache into a HashMap in memory, and joining with a larger dataset.

Hadoop command to run this :

hadoop jar /home/cloudera/Downloads/MapJoin.jar com.hadoop.mapReduce.MapSideJoin.DriverMapSideJoinDCacheTxtFile /user/cloudera/Joins_Input/employees.txt /user/cloudera/Joins_Input/departments.txt /user/cloudera/Joins_OutPut

Joins:-
=======

Joins is one of the interesting features available in MapReduce. Joins performed by Mapper are called as Map-side Joins.
Joins performed by Reducer can be treated as Reduce-side joins. Frameworks like Pig, Hive, or Cascading has support for performing joins.

Before diving into the implementation let us understand the problem throughly. If we have two datasets, for example,
one dataset having user ids, names and the other having the user activity over the application. In-order to find out which user have performed what activity on the application we might need to join these two datasets such as both user names and the user activity will be joined together.

Join can be applied based on the dataset size if one dataset is very small to be distributed across the cluster then we can use Side Data Distribution technique.

Side Data Distribution:-
====================
Side-Data is the additional data needed by the job to process the main dataset. The critical part is to make this side-data available to all the map or reduce tasks running in the cluster. It is possible to cache the side-data in memory in a static field, so that the tasks running successively in a task tracker will share the data. Using the Task JVM re-use feature we can handle this. When using this feature we should make sure that the amount of memory needed to cache the data should not affect the memory needed for Shuffle and sort phase.

Caching of side data can be done in two ways,

Job Configuration:- Using Job Configuration object setter method we can set the key-value pairs and the same can be retrieved in the map or reduce tasks. We should be careful using this option to not to use huge amount of data to be shared in this way since the configuration is read by the JobTracker, TaskTracker and the child JVMs and everytime the configurations will be loaded into the memory.

Distributed Cache:-
=================
Side-Data can be shared using the Hadoop’s Distributed cache mechanism. We can copy files and archives to the task nodes when the tasks need to run. Usually this is the preferrable way over the JobConfigurtion.

If both the datasets are too large then we cannot copy either of the datasets to each node in the cluster as we did in the Side data distribution. We can still join the records using MapReduce with a Map-side or reduce-side joins.


-----------------------------------------------------------------------------------------------------------------------------

You can reach me for any suggestions/clarifications on  : revanthkumar95@gmail.com                                              
Feel free to share any insights or constructive criticism. Cheers!!                                                           
#Happy hadooping!  
