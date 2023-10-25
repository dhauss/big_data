# Assignment 2: PySpark

## Question 1: Show total number bought per item, per day, between 11am and 1pm:

Program first reads in the BreadBasket_DMS csv file and filters out all NONE entries before moving on to part 1-specific processing. It proceeds to filter out all transactions outside of the specified time range (11:00-13:00 inclusive), and then groups the result first by Item, then by Date, and creates a "Quantity" column by counting the entries. This results in a dataframe that show the total number of each item bought per day between 11:00 and 13:00 inclusive

SAMPLE ANSWER:

|             Item|      Date|Quantity|

|         Focaccia|2016-11-03|       1|

|          Tartine|2016-11-04|       1|

|            Bread|2016-12-13|       4|

|           Coffee|2017-01-05|       7|

|     Scandinavian|2017-01-20|       1|

|         Art Tray|2017-01-24|       1|

|            Bread|2017-03-22|       6|

| Coffee granules |2017-03-25|       1|

|           Muffin|2016-11-14|       1|

|           Coffee|2016-11-24|       7|

|Gingerbread syrup|2016-12-21|       1|

|           Coffee|2017-01-06|       8|

|     Scandinavian|2017-01-07|       3|



## Question 2: Show the top 3 (by qty) items bought by Daypart, by DayType

The first cell (Cmd 5) defines the UDFs that will be used to create the columns Daypart (weekend/weekday) and Daytype (Morning/Afternoon/Night). It then uses these UDFs to add the columns to the bakery_data DF that already had the NONE entries removed. The next step groups this DF by Daypart, Daytime, and Item to count the number of each item purchased for every possible Daytime/Daypart combination. A window function is then used to partition by Daypart/Daytime and order by total purchases per item, with the output limited to the top 3 purchases for each Daypart/Daytime. The last two transformations modify the format so that the top 3 items per Daypart/Daytime are collected in a list and displayed on a single row, and the final transformation reorders the columns for readability before calling show()

ANSWER:

+---------+------------------------------------------+-------+
|Daypart  |Top_3_Items                               |Daytype|
+---------+------------------------------------------+-------+
|Afternoon|[Coffee, Bread, Tea]                      |Weekday|
|Afternoon|[Coffee, Bread, Tea]                      |Weekend|
|Morning  |[Coffee, Bread, Pastry]                   |Weekday|
|Morning  |[Coffee, Bread, Pastry]                   |Weekend|
|Night    |[Coffee, Bread, Tea]                      |Weekday|
|Night    |[Coffee, Tshirt, Afternoon with the baker]|Weekend|
+---------+------------------------------------------+-------+

## Question 3: The total number of entities by “rpt_area_desc”

The first cell (Cmd 7) reads in the European style CSV file, defining the semicolon delimiter in read options. The resulting DF is then simply grouped by the Rpt_Area_Desc column, counted to a column 'Total', and sorted in descending order. The final DF is then shown with the top 3 results

ANSWER:

+--------------+-----+
| Rpt_Area_Desc|Total|
+--------------+-----+
|  Food Service| 1093|
|Swimming Pools|  420|
|   Summer Food|  242|
+--------------+-----+

## Question 4: Show the top 10 regions with the biggest percentage decrease in population, for the years 1990-2000

The first cell (Cmd 9) is dedicated to data cleanup. The column name for region was missing from the header, so the columns attribute is used to find the default name and rename the first column to 'Region'. Some entries included either 'NA' or '--' population data, so these entries were filtered out to leave only entries with valid population numbers. Furthermore, there were 7 aggregate regions listed. The Former USSR was already removed by the previous filter, but the other 6 had to be manually filtered out.

After cleaning, I first calculated the gross increase for each region and created a new column 'gross_increase'. I then filtered out all positive population increases as they would not be among the top 10 population decreases. I then calculated the percent increases, saved them in a column 'perc_increase', and sorted the data on 'perc_increase' while saving only the Region and perc_increase columns to a new dataframe. Note that a negative perc_increase denotes a population decrease. For the final result, I showed the top 10 results from this sorted DF.

ANSWER:

+-------------+--------------------+
|       Region|       perc_increase|
+-------------+--------------------+
|   Montserrat| -0.6318732525629077|
|     Bulgaria|-0.12092718374010437|
| Cook Islands|-0.11310494834148986|
| Sierra Leone|-0.09912965328035572|
|       Kuwait|-0.07841983884671756|
|    Gibraltar| -0.0632085194091378|
|Faroe Islands|-0.03478077571669489|
|      Albania|-0.02668162333239...|
|      Hungary|-0.02164024265610...|
|      Romania|-0.01830669620112...|
+-------------+--------------------+

## Question 5: Word Count

In the first cell (Cmd 11), I first read in the data and normalize the words to lower case, replacing all punctuation with white spaces. I then split the words values by space to create an array of single words and explode the column to create a DF where every single word from the input has its own row. I overwrite the original words column to avoid duplicate data when exploding. I then simply group by the words column and call count to create a new DF with all unique words and their respective number of occurrences ('count') and show the results.

SAMPLE ANSWER:

+------------+-----+
|       words|count|
+------------+-----+
|       trail|   57|
|       those| 3409|
|    medicare|   32|
|        some| 4335|
|         few| 1057|
|   connected|  162|
| herzegovina|    7|
|   involving|   99|
|    randomly|   10|
|     clinics|   78|
|       still| 2139|
| transmitted|   92|
|      travel| 1367|
+____________+_____+

## Question 6: 10 Most Common Bigrams

In the first cell (Cmd 13), I transform the parsed words column, which was normalized and had its punctuation removed but had not yet been exploded, and convert each row into an array of strings using 'split'. This is a requirement for using the NGram feature from the pyspark.ml.feature library. I define the bigram transformation, as well as the input and output columns, before applying the transformation on this DF. The result is a DF where all rows are an array of bigrams as well as the original array of word strings. I remove the 'words' input column before exploding the bigram column to obtain a DF where each row is a bigram. I then group by bigram and count in order to count the unique bigrams in the DF. After this, I sort the resulting DF by the count column in descending order and show the first 10 columns to provide the solution

ANSWER:

| bigrams|count|
+--------+-----+
|  of the|17484|
|  in the|12808|
|   p the|10363|
|covid 19| 8762|
|  to the| 8372|
| for the| 5588|
|     n t| 5393|
|  on the| 5032|
|   to be| 4581|
| will be| 4177|
+________+_____+

## Question 7

I first defined a UDF to use in both parts a and b. It takes in two columns of coordinate strings, converts the strings to tuples of floats and passes them to the haversine function with the units defined as miles. It returns a column of the haversine distance between the two coordinates in miles

# Part A: Find the food service and active restaurant closest to the following coordinates: 35.994914, -78.897133

I first filter the rest_data_raw DF from Q3 to define a new DF with only the active, food service restaurants. I then drop any rows with null geolocation values, as these null values would break the haversine UDF. I then use the haversine UDF to create a new column, 'Distance(mi)', passing in a column of the origin coordinates using the lit() function as well as the geolocation column. I obtain the final results by sorting this DF, and then show the top result with only the columns 'Premise_name', 'Status', 'Rpt_Area_Desc', 'geolocation', and 'Distance(mi)' for readability.

ANSWER: OLD HAVANA SANDWICH SHOP| ACTIVE | Food Service | 35.9932826, -78.8981331 | 0.1258222


# Part B

I save the coordinates from the part a's result using collect(), and then read in the json dataset. I create a DF with only the geocode subfield from this dataset, as that is all that is required to answer the question. I again remove rows with null values for coordinates. Because my original haversine UDF accepts strings, I cast the geocode column, originally a list of floats, to a string column and remove the brackets. The list values would require some casting either way to be able to use the haversine function, so this seemed to be an acceptable, though somewhat bulky, solution. I again create a 'Distance(mi)' column in the same way as part A, and proceed to filter out all distances over 1 mile. For the final result, I simply print out the count of the resulting DF

ANSWER: 320

