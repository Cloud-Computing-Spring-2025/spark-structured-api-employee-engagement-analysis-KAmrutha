from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark_session(app_name="Identify_High_Satisfaction_Departments"):
    """
    Initialize and return a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_employee_data(spark, file_path):
    """
    Load employee data from a CSV file into a Spark DataFrame.
    """
    schema = """
        EmployeeID INT, 
        Department STRING, 
        JobTitle STRING, 
        SatisfactionRating INT, 
        EngagementLevel STRING, 
        ReportsConcerns BOOLEAN, 
        ProvidedSuggestions BOOLEAN
    """
    return spark.read.csv(file_path, header=True, schema=schema)

def filter_high_satisfaction_departments(employee_df):
    """
    Identify departments where more than 50% of employees have a Satisfaction Rating > 4
    and an Engagement Level of 'High'.
    """
    high_satisfaction_df = employee_df.filter(
        (col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High")
    )
    
    department_counts_df = employee_df.groupBy("Department").count().withColumnRenamed("count", "TotalEmployees")
    
    high_satisfaction_counts_df = high_satisfaction_df.groupBy("Department").count().withColumnRenamed("count", "HighSatisfactionEmployees")
    
    department_summary_df = department_counts_df.join(
        high_satisfaction_counts_df, on="Department", how="left"
    ).fillna(0)
    
    percentage_df = department_summary_df.withColumn(
        "HighSatisfactionPercentage", 
        spark_round((col("HighSatisfactionEmployees") / col("TotalEmployees")) * 100, 2)
    )
    
    return percentage_df.filter(col("HighSatisfactionPercentage") > 7.5).select("Department", "HighSatisfactionPercentage")

def save_results_to_csv(result_df, output_file):
    """
    Save the resulting DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_file, header=True, mode='overwrite')

def main():
    """
    Main execution function.
    """
    spark = initialize_spark_session()
    
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-KAmrutha/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-KAmrutha/outputs/task1/departments_high_satisfaction.csv"
    
    employee_df = load_employee_data(spark, input_file)
    
    high_satisfaction_departments_df = filter_high_satisfaction_departments(employee_df)
    
    save_results_to_csv(high_satisfaction_departments_df, output_file)
    
    spark.stop()

if __name__ == "__main__":
    main()
