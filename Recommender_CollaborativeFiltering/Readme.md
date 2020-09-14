# Memory/Item-Based Collaborative Filtering Recommender 

MapReduce v2 Application for School of Computing HDFS cluster running YARN.  

## Read up on Collaborative-Filtering
- https://en.wikipedia.org/wiki/Collaborative_filtering
- https://towardsdatascience.com/various-implementations-of-collaborative-filtering-100385c6dfe0

## Application broken down to 2 MR jobs
- Job 1 - creates Co-occurence Matrix. 
- Job 2 - Creates User Vector and does dot product on appropriate rows. 

## Caveats and Future Improvements: 
1. If data too big, multiple reducers will occur. This will not let us produce the NxN Co-occurence matrix, where N is the total number of items. 
2. We spit out the sparse COMatrix, and then take it back up in Job 2. This is inefficient (and an inherent bottleneck of MR). Need to find a better way. 
3. Make the imperative code more efficient in places. 

## Instructions Given by prof: 
############### Assignment 1-2: Recommendation  #####################

Command Format: Recomend <input_file> <output_dir>
Example: hadoop jar cf.jar Recommend recommendation/input/data.csv recommendation/output
(All the file path in the command is HDFS path)
The output should be stored in a text file recommendation/output/part-r-00000 by default.

Use scripts to compile and submit your codes
$ ./compile_run		# compile and run your code on sample dataset, check the result
These scripts will also be used on marking. So ensure your output format is the same as 'answer.txt'

In this assignment, you should ONLY modify:
-- src
|-- Recommend.java
|-- (You can add other java files)

Do NOT define Java packages. The script compiles everything under 'src/' as simple java files.

