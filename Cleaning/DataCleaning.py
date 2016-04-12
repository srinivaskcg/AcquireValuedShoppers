# Please Start the spark as "pyspark --packages com.databricks:spark-csv_2.10:1.0.3"
from dateutil.parser import parse
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
import os

path = os.getcwd()
transactionFile = sc.textFile(path+"/Chunk1_transactions.csv")
sqlContext = SQLContext(sc)
schemaString = "CUSTOMER_ID CHAIN_ID DEPARTMENT_ID CATEGORY_ID COMPANY_ID BRAND_ID PURCHASE_DATE PRODUCT_SIZE PRODUCT_MEASURE PURCHASE_QTY PURCHASE_AMT"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
fields[6].dataType = TimestampType()
fields[7].dataType = FloatType()
fields[9].dataType = FloatType()
fields[10].dataType = FloatType()
schema = StructType(fields)
header = transactionFile.filter(lambda line: "id" in line)
transactionFile = transactionFile.subtract(header)
transactions = transactionFile.map(lambda l: l.split(",")).map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],parse(p[6]),float(p[7]),p[8],float(p[9]),float(p[10])))
transactions_df = sqlContext.createDataFrame(transactions, schema) 
transactions_df.registerTempTable("transactions")

#Cleaning the values in transactionsDF
transactions_df = sqlContext.sql("select * from transactions where PURCHASE_AMT != 0 or PURCHASE_AMT !=null or PURCHASE_QTY != 0 or PURCHASE_QTY > 0 or PURCHASE_QTY != null or CATEGORY_ID != null or COMPANY_ID != null or BRAND_ID != null")
transactions_df.repartition(1).write.format("com.databricks.spark.csv").save(path+"Chunk1")