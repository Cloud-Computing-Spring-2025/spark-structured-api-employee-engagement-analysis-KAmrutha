from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark_session(app_name="Valued_Employees_No_Suggestions"):
    
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_employee_data(spark, file_path):
   
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

def analyze_valued_employees_without_suggestions(employee_df):
   
    valued_employees_df = employee_df.filter(col("SatisfactionRating") >= 4)
    
    valued_no_suggestions_df = valued_employees_df.filter(col("ProvidedSuggestions") == False)
    
    total_valued_no_suggestions = valued_no_suggestions_df.count()
    total_employees = employee_df.count()
    proportion_no_suggestions = round((total_valued_no_suggestions / total_employees) * 100, 2) if total_employees > 0 else 0.0
    
    return total_valued_no_suggestions, proportion_no_suggestions

def save_analysis_results(number_of_employees, percentage, output_file):
   
    with open(output_file, 'w') as file:
        file.write(f"Number of Valued Employees Without Suggestions: {number_of_employees}\n")
        file.write(f"Proportion: {percentage}%\n")

def main():
   
    spark = initialize_spark_session()
    
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-KAmrutha/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-KAmrutha/outputs/task2/valued_no_suggestions.csv"
    
    employee_df = load_employee_data(spark, input_file)
    
    total_valued_no_suggestions, proportion_no_suggestions = analyze_valued_employees_without_suggestions(employee_df)
    
    save_analysis_results(total_valued_no_suggestions, proportion_no_suggestions, output_file)
    
    spark.stop()

if __name__ == "__main__":
    main()
