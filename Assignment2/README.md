# Assignment 2: PySpark

## Question 1: Show total number bought per item, per day, between 11am and 1pm:

Program first reads in the BreadBasket_DMS csv file and filters out all NONE entries before moving on to part 1-specific processing. It proceeds to filter out all transactions outside of the specified time range (11:00-13:00 inclusive), and then groups the result first by Item, then by Date, and creates a "Quantity" column by counting the entries. This results in a dataframe that show the total number of each item bought per day between 11:00 and 13:00 inclusive

## Question 2: Show the top 3 (by qty) items bought by Daypart, by DayType

The first cell (Cmd 5) defines the UDFs that will be used to create the columns Daypart (weekend/weekday) and Daytype (Morning/Afternoon/Night). It then uses these UDFs to add the columns to the bakery_data DF that already had the NONE entries removed. The next step groups this DF by Daypart, Daytime, and Item to count the number of each item purchased for every possible Daytime/Daypart combination. A window function is then used to partition by Daypart/Daytime and order by total purchases per item, with the output limited to the top 3 purchases for each Daypart/Daytime. The last two transformations modify the format so that the top 3 items per Daypart/Daytime are collected in a list and displayed on a single row, and the final transformation reorders the columns for readability before calling show()

## Question 3: The total number of entities by “rpt_area_desc”

The first cell (Cmd 7) reads in the European style CSV file, defining the semicolon delimiter in read options. The resulting DF is then simply grouped by the Rpt_Area_Desc column, counted to a column 'Total', and sorted in descending order. The final DF is then shown with the top 3 results


## Question 4: Show the top 10 regions with the biggest percentage decrease in population, for the years 1990-2000

The first cell (Cmd 9) is dedicated to data cleanup. The column name for region was missing from the header, so the columns attribute is used to find the default name and rename the first column to 'Region'. Some entries included either 'NA' or '--' population data, so these entries were filtered out to leave only entries with valid population numbers. Furthermore, there were 7 aggregate regions listed. The Former USSR was already removed by the previous filter, but the other 6 had to be manually filtered out.

After cleaning, I first calculated the gross increase for each region and created a new column 'gross_increase'. I then filtered out all positive population increases as they would not be among the top 10 population decreases. I then calculated the percent increases, saved them in a column 'perc_increase', and sorted the data on 'perc_increase' while saving only the Region and perc_increase columns to a new dataframe. Note that a negative perc_increase denotes a population decrease. For the final result, I showed the top 10 results from this sorted DF.

## Question 5: Word Count

In the first cell (Cmd 11), I first read in the data and normalize the words to lower case, replacing all punctuation with white spaces. I then split the words values by space to create an array of single words and explode the column to create a DF where every single word from the input has its own row. I overwrite the original words column to avoid duplicate data when exploding. I then simply group by the words column and call count to create a new DF with all unique words and their respective number of occurrences ('count') and show the results.

## Question 6: 10 Most Common Bigrams

In the first cell (Cmd 13), I transform the parsed words column, which was normalized and had its punctuation removed but had not yet been exploded, and convert each row into an array of strings using 'split'. This is a requirement for using the NGram feature from the pyspark.ml.feature library. I define the bigram transformation, as well as the input and output columns, before applying the transformation on this DF. The result is a DF where all rows are an array of bigrams as well as the original array of word strings. I remove the 'words' input column before exploding the bigram column to obtain a DF where each row is a bigram. I then group by bigram and count in order to count the unique bigrams in the DF. After this, I sort the resulting DF by the count column in descending order and show the first 10 columns to provide the solution

## Question 7

