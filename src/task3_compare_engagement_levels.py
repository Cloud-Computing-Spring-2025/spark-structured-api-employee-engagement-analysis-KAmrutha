from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, round as spark_round

def initialize_spark_session(app_name="Compare_Job_Engagement_Levels"):
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

def assign_engagement_scores(employee_df):
    """
    Convert EngagementLevel from categorical to numerical values.
    """
    return employee_df.withColumn("EngagementScore", 
                                  when(col("EngagementLevel") == "Low", 1)
                                  .when(col("EngagementLevel") == "Medium", 2)
                                  .when(col("EngagementLevel") == "High", 3)
                                  .otherwise(0))

def calculate_avg_engagement_by_job(employee_df):
    """
    Calculate the average engagement score per job title.
    """
    return employee_df.groupBy("JobTitle").agg(spark_round(avg("EngagementScore"), 2).alias("AvgEngagementScore"))

def save_engagement_results(engagement_df, output_file):
    """
    Save engagement level comparisons to a CSV file.
    """
    engagement_df.coalesce(1).write.csv(output_file, header=True, mode='overwrite')

def main():
    """
    Main execution function.
    """
    spark = initialize_spark_session()
    
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-KAmrutha/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-KAmrutha/outputs/task3/compare_engagement_levels.csv"
    
    employee_df = load_employee_data(spark, input_file)
    
    employee_df_scored = assign_engagement_scores(employee_df)
    
    avg_engagement_df = calculate_avg_engagement_by_job(employee_df_scored)
    
    save_engagement_results(avg_engagement_df, output_file)
    
    spark.stop()

if __name__ == "__main__":
    main()
