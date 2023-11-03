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