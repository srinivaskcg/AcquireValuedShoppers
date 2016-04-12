1. Divide the transaction file into chunks of 4GB. 
2. Run Datacleaning script on each chunk., which results in a output file for each chunk. 
3. Outputs of each chunk are merged into single file using the following command
		Command: find . -type f -name 'part*' -exec cat {} + >> transactions_cleaned.csv
4. Reductions are carried out on the merged files