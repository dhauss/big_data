# Summary
Bigram counting and probability program to be run in Hadoop MapReduce. Takes in a text file or directory of text files, outputs a directory of text files containing all unique Bigrams, one containing all unique Bigrams and their probability of occurring in the input file(s), and a directory with a single text file containing the most likely bigram beginning with the word 'possible' and its probability of occurring in the input file(s). To run in HDFS, create and upload the jar file, then run the following command:

hadoop jar \<jar file\> bigram.probs.Driver \<input file\> \<output file\>

The program consists of three separate jobs. countJob counts the occurrence of each unique bigram and tracks a global counter of all bigrams in the input, outputting to the directory 'count-job-out'. probJob is a mapper only job that reads in the global counter from countJob and maps each Bigram to the probability of its occurrence in the input file. The output directory is 'prob-job-out'. The final job, possibleJob, finds the most likely bigram starting with the word 'possible'. It first filters the input to find only bigrams starting with the word 'possible', then a combiner class finds the most likely 'possible' bigram local to the mapper node, and finally finds the global max in the reducer. The output directory is specified by the user, and will consist of a single text file with the most likely 'possible' bigram as well as its probability


# Main Classes

Driver:
Driver program responsible for configuring the MapReduce jobs. Intermediate output files defined as 'count-job-out' and 'prob-job-out', automatically deletes existent intermediate files. Passes global Hadoop counter from countJob to probJob, and sets 0 reducers for probJob and 1 reducer for possibleJob to reduce overhead and empty output files. 

Bigram:
A custom bigram class, contains two private Text attributes representing the First and Second word in a bigram. Implements writeableComparable for serialization and sorting in HDFS. 

CountMapper:
Parses out punctuation, HTML tags, and consecutive white space, then maps each bigram in the input to key: bigram, val: 1. Also increments a global Hadoop counter to track total number of bigrams in file

CountReducer:
Simple counting reducer, iterates through and sums all values for a given Bigram key. Effectively counts the number of occurrences of each unique bigram in input

ProbMapper:
Maps each unique bigram to key: bigram, val: probability, calculated by dividing its count by the global count from countJob. Probability is represented as a float

PossibleMapper:
Filters out all bigrams that do not begin with the word 'possible', then maps the 'possible' bigrams to key: 'possible', val: 'second/tprobability', both represented as Text objects. This allows all bigrams to be mapped to the same key after significant filtering so that the reducer can find a global max

PossibleCombiner:
Largely the same function as the PossibleReducer, finds local max 'possible' bigram and filters out all other 'possible' bigrams before passing on to the reducer. The only significant difference is that the output format matches that of PossibleMapper. Use of this combiner class reduces the bottleneck caused by mapping all values to a single key

PossibleReducer:
Iterates through all local max 'possible' bigrams, outputs only the global max as key: Bigram, val: probability

# Test Cases
Basic functionality MRUnit tests to ensure mapper and reducer classes are correct before runnings on HDFS