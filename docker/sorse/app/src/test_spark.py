import pyspark
from pyspark.sql import SparkSession

print("<<---***--- START ---***--->>")

spark = (
    SparkSession.builder
        .appName("Word Count")
        .master("spark://10.6.0.7:7077")
        .getOrCreate()
)

rows = [
(1,"clothes"), (2,"games"), (3,"electronics"),
(4,"cars"), (5,"travel"), (6,"books")
]

schema = "ad_id BIGINT, category STRING"
adCategoryDF = spark.createDataFrame(rows, schema)
adCategoryDF.show()

adCategoryDF.repartition(1).write \
            .format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .option("sep", "\t") \
            .save("./files/tests")
            
print("<<---***--- END ---***--->>")