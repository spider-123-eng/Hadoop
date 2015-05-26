#Filtering out duplicate images using Map Reduce
                          
Let's take a scenario where we have a collection of millions of image files, but in those collection only few thousands are unique images and others are duplicates.

Our purpose is to filter out those unique images. And we need to write map reduce algorithm to accomplish this task.


To achieve this task we need to create the tar file of all images. This can be a single tar or multiple tar files. Once tar files are created we need to create sequence file out of tar files.

Then follow the steps below in that sequence. 

Step 1: Convert the tar files into sequence files. Tar files can be converted into sequence file using the tar-to-seq tool provided here. 

If you have tar of images then run the following comment(You should tar-to-seq.jar file in your local system to convert .tar file in to .seq file).

java -jar tar-to-seq.jar images.tar images.seq

Step 2: Copy the sequence files to HDFS, which will create the blocks and store it in data nodes.

Step 3: Write map reduce algorithm

The input to mapper will be file name as key and file content as value. All the duplicate images will have same content. So, if we spit out image content as value, all images with same content will be grouped together in the same reducers. And then in the reducer we can pick one file and filter out those images.

The mapper, reduce and driver code is given below.




