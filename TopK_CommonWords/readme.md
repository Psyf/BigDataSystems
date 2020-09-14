# Top K Common Words Across 2 Files

MapReduce v2 Application running on School of Computing YARN/HDFS Cluster.

## Explanation
- MR Job 1 counts File 1
- MR Job 2 counts File 2
- MR Job 3 takes outputs of each, and takes min(f1, f2) and spills results to disk. 

## Caveats and Improvements: 
1. Only works for 2 files. Need to modify to handle arbitrary number of files. 
2. Job 3 assumes everything will fit in 1 Reducer - might not be true && bottleneck. 
3. Imperative code not the cleanest because I suck at Java!

## Instructions given by Prof: 
############### Assignment 1-1: Top K Common Words #####################

Command Format: TopkCommonWords <input_file1> <input_file2> <stopwords> <output_dir>
Example: hadoop jar cm.jar TopkCommonWords commonwords/input/task1-input1.txt commonwords/input/task1-input2.txt commonwords/input/stopwords.txt commonwords/cm_output/
(All the file path in the command is HDFS path)
The output should be stored in a text file commonwords/cm_output/part-r-00000 by default.

Use scripts to compile and submit your codes
$ ./compile_run		# compile and run your code on sample dataset, check the result
$ ./submit		# submit your codes (You can submit multiple times before due time)
These scripts will also be used on marking. So ensure your output format is the same as 'answer.txt'

In this assignment, you should ONLY modify:
-- TopkCommonWords.java

Do NOT add new files. Do NOT define Java packages. The script will compile 'TopkCommonWords.java' as simple java file.
