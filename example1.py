# importing necessary libraries 
from pyspark.sql import SparkSession 
  
# function to create new SparkSession 
def create_session(): 
  spk = SparkSession.builder \
      .master("local") \
      .appName("Geek_examples.com") \
      .getOrCreate() 
  return spk 
  
# main function 
if __name__ == "__main__": 
  
  # calling function to create SparkSession 
  spark = create_session() 
  
  #  creating data for creating dataframe  
  data = [ 
    ("Shivansh","M",50000,2), 
    ("Vaishali","F",45000,3), 
    ("Karan","M",47000,2), 
    ("Satyam","M",40000,4), 
    ("Anupma","F",35000,5) 
  ] 
  
  # giving schema 
  schm=["Name of employee","Gender","Salary","Years of experience"] 
  
  # creating dataframe using createDataFrame() 
  # function in which pass data and schema 
  df = spark.createDataFrame(data,schema=schm)

  df.select("gender","salary").show() 
  df.select("salary","years of experience").show()
  # visualizing the dataframe using show() function 
  df.show()