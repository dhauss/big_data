Bigram counting and probability program to be run in Hadoop MapReduce. Takes in a text file or directory of text files, outputs a directory of text files containing all unique Bigrams, one containing all unique Bigrams and their probability of occurring in the input file(s), and a directory with a single text file containing the most likely bigram beginning with the word 'possible' and its probability of occurring in the input file(s). To run in HDFS, create and upload the jar file, then run the following command:

	hadoop jar <jar file> bigram.probs.Driver <input file> <output file>

