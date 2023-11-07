# Hadoop MapReduce Reservoir Sampling
Implements the reservoir sampling algorithm for Hadoop MapReduce where K = 10000 (saved as const variables in the mapper and reducer classes). Designed to receive csv logs as input, will output K sample rows into a single output file enumerated by row

To run in HDFS, run the following command:

hadoop jar ReservoirSampling-0.0.1-SNAPSHOT.jar res.sampling.ResDriver \<input directory\> \<output directory\>

Log data from Backblaze\
Source: https://www.backblaze.com/blog/backblaze-hard-drive-stats-q1-2019/ \
Reference: https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data\

## ResDriver

Simple job configuration, implements Tool for command line HDFS configuration, takes input and output directories or files as input and runs implementation of reservoir sampling algorithm. It first checks for correct command line arguments, then initializes the job object, setting ResDriver as the jar class, ResMapper as the Mapper class, and ResReducer as the Reducer class. It sets a single reducer task to avoid unnecessary overhead from inactive reducers, and sets the respective Mapper and Reducer output keys and values. After setting the input and output paths specified on the command line, it returns 0 on successful completion of the job or 1 in case of failure.

## ResMapper

First splits the csv input on linebreaks to obtain an array of rows. It then iterates through the rows, incrementing an integer i which enumerates the current row, and adding the first K rows to a reservoir array. When the reservoir is filled, a random integer, j, is generated between 0 (inclusive) and i (exclusive). If j is greater than or equal to K, the row is ignored. If j is less than K, the current row replaces the jth entry in the reservoir.

After iterating through all input rows, the reservoir is then written to output. A null check ensures that no null values are written to output if the reservoir is not full, a possibility if a given split consists of fewer than K records. A NullWritable key is used to map all values to a single reducer, which will handle M * K rows, M being the number of mappers used by the MapReduce job. This is an essentially constant number of entries handled by the reducer, as K = 10000 and the number of mappers is configurable and generally limited to less than 50 mappers in this context.

For readability, multiple commas are replaced with single commas due to the high number of null values in this particular dataset (see source above). Note that this parsing (ResMapper line 50) should not be applied if the output will be used as a csv file. It was a purely aesthetic choice made in the specific context of the project.

## ResReducer

Largely the same functionality as ResMapper. Takes M * K random samples, M being the number of mappers, and applies the reservoir sampling algorithm to produce the final K samples. The only major differences are that the rows are handled as Text objects, which are reinitialized locally to avoid Hadoop object pointer bugs, and the output key is a sequential integer which enumerates 1-indexed rows. This is mainly a tool for ensuring the output quantity is correct, and could easily be replaced with null keys to maintain the csv format. 

