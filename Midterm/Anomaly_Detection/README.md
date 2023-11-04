%md
<h1 style="text-align: center;">
Anomaly Detection 
</h1>

1. For each model, compute the bi-monthly failure rate R per model: F = number of failures per model, O = number of cumulative days in operation per model, D = number of days between Jan 1, 2019 and March 28, 2019, inclusive:


$R = 100.0 * \left(\frac{1.0 * F}{O \div D}\right)$


2. Given R per model, find the mean M and standard deviation S

3. Use M, S to predict which models in operation on March 29, 2019 will fail, with failure
predicted if the model’s R exceeds M + 1S

Log data from Backblaze\
Source: https://www.backblaze.com/blog/backblaze-hard-drive-stats-q1-2019/ \
Reference: https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data\

<h3 style="text-align: center;">
Basic Data Cleaning
</h3>
Reads in the raw data, selects only the columns needed for anomaly detection analysis and filters out all rows outside of specified date range

<h3 style="text-align: center;">
Calculate Failures (F) and Accumulated Days of Operation (O)
</h3>

Groups by model, then adds two columns: every entry where a model does not fail is counted as one day of operation (O), every entry where a model fails is counted as a failure (F). Earlier data exploration showed that hard disks do not have logs for every day between their first entry and their failure entry, so assumption is that the hard disks were only active on days for which they have logs. Dataframe is then persisted as all subsequent actions will require this dataframe, and this avoids having to continuously recalculate the log_df_grouped dataframe from the significantly larger log_df_raw dataset

<h3 style="text-align: center;">
Calculate failure rate
</h3>
Failure rate is calculated according to formula provided in prompt using the lit function to insert the DATE_RANGE_DAYS const calculated in cmd 2

<h3 style="text-align: center;">
Calculate Mean, Standard Deviation, Failure Prediction Threshold
</h3>

Mean and standard deviation are calculated using built-in PySpark functions and saved to a dataframe. They are then saved as const variables, summed and assigned to the FAIL_PREDICT const variable in order to create dataframe of failure predictions in the final cell. The final lines of the cell simply prints each variable for manual error checking

<h2 style="text-align: center;">
Hard Drive Failure Prediction Final Result
</h2>

The final result is calculated by simply filtering out all models at or below the failure rate threshold and showing the resulting dataframe, sorted in descending order by likelihood of failure