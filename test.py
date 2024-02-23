
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("CompanyDataExample").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

# Sample data
data = [
    (1, "John", "Doe", "HR", 60000),
    (2, "Alice", "Smith", "IT", 70000),
    (3, "Bob", "Johnson", "Finance", 80000),
    (4, "Emma", "Williams", "Marketing", 75000),
    (5, "Charlie", "Brown", "IT", 90000),
    (6, "Olivia", "Miller", "Finance", 82000),
    (7, "James", "Davis", "Marketing", 78000),
    (8, "Sophia", "Wilson", "HR", 65000),
    (9, "Liam", "Moore", "IT", 95000),
    (10, "Ava", "Taylor", "Finance", 83000)
]

# Create a PySpark DataFrame
company_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
company_df.show()

# Stop the Spark session
spark.stop()
