# Hadoop MapReduce Reservoir Sampling
Implements the reservoir sampling algorithm for Hadoop MapReduce where K = 10000 (saved as const variables in the mapper and reducer classes). Designed to receive csv logs as input, will output K sample rows into a single output file

To run in HDFS, run the following command:

hadoop jar rs.jar res.sampling.ResDriver \<input directory\> \<output directory\>

## ResDriver

## ResMapper

First splits the csv input on linebreaks to obtain an array of rows. It then iterates through the rows, incrementing an integer i which enumerates the current row, and adding the first K rows to a reservoir array. When the reservoir is filled, a random integer, j, is generated between 0 (inclusive) and i (exclusive). If j is greater than or equal to K, the row is ignored. If j is less than K, the current row replaces the jth entry in the reservoir.

After iterating through all input rows, the reservoir is then written to output. A null check ensures that no null values are written to output if the reservoir is not full, a possibility if a given split consists of fewer than K records. An arbitrary key of 1 is used to map all values to a single reducer, which will handle M * K rows, M being the number of mappers used by the MapReduce job. This is an essentially constant number of entries handled by the reducer, as K = 10000 and the number of mappers is configurable and generally limited to less than 50 mappers in this context.

For readability, multiple commas are replaced with single commas due to the high number of null values in this particular dataset. Note that this parsing (ResMapper line 50) should not be applied if the output will be used as a csv file. It was a purely aesthetic choice made in the specific context of the project.

## ResReducer