# Hadoop

Goal:

I want to be able to specify the number of mappers used on an input file
Equivalently, I want to specify the number of line of a file each mapper will take
Simple example:

For an input file of 10 lines (of unequal length; example below), I want there to be 2 mappers -- each mapper will thus process 5 lines.

Sample Input :  
This is  
an arbitrary example file   
of 10 lines.   
Each line does   
not have to be   
of   
the same   
length or contain   
the same   
number of words   


The attached code, using the above example data, will produce

map 10    

I want the output to be  

map 2  
where the first mapper will do something will the first 5 lines, and the second mapper will do something with the second 5 lines.



-----------------------------------------------------------------------------------------------------------------------------

You can reach me for any suggestions/clarifications on  : revanthkumar95@gmail.com                                              
Feel free to share any insights or constructive criticism. Cheers!!                                                           
#Happy hadooping!  
