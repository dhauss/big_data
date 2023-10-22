#Assignment 2: PySpark

## Part 1 

show total number bought per item, per day, between 11am and 1pm

Read in the BreadBasket_DMS csv file and filtered out all NONE entries before moving on to part 1-specific processing

## Part 2


## Part 3

European csv with semicolon delimiter specified in read options, then simply grouped by the rpt area desc column, counted and sorted in descending order before showing the top 3


## Part 4

Clean up required more work: column name for region was missing, so rescued col name using the columns attribute and renamed it to Region. Some entries included either 'NA' or '--' population data, removed these entries and left only entries with valid population numbers. Furthermore, there were 7 aggregate regions listed, Former USSR was already removed by previous filter but manually removed the other 6.

After cleaning the data, first calculated the gross increase and created a new column 'gross_increase', filtered out all positive population increases as they clearly would not be in the top 3. I then calculated the percent increases, saved them in a column 'perc_increase', and sorted the data on 'perc_increase' while saving only the Region and perc_increase columns to a new dataframe. For the final result, I showed the top 3 results from this sorted dataframe