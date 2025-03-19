from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os
from datetime import datetime

# Schema declaration
restaurant_schema = StructType([
    StructField("CAMIS", IntegerType(), False),
    StructField("DBA", StringType(), True),
    StructField("BORO", StringType(), True),
    StructField("BUILDING", StringType(), True),
    StructField("STREET", StringType(), True),
    StructField("ZIPCODE", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("CUISINE DESCRIPTION", StringType(), True),
    StructField("INSPECTION DATE", StringType(), True),
    StructField("ACTION", StringType(), True),
    StructField("VIOLATION CODE", StringType(), True),
    StructField("VIOLATION DESCRIPTION", StringType(), True),
    StructField("CRITICAL FLAG", StringType(), True),
    StructField("SCORE", IntegerType(), True),
    StructField("GRADE", StringType(), True),
    StructField("GRADE DATE", StringType(), True),
    StructField("RECORD DATE", StringType(), True),
    StructField("Community Board", StringType(), True)
])

population_schema = StructType([
    StructField("Borough", StringType(), True),
    StructField("CD Number", IntegerType(), True),
    StructField("CD Name", StringType(), True),
    StructField("1970 Population", IntegerType(), True),
    StructField("1980 Population", IntegerType(), True),
    StructField("1990 Population", IntegerType(), True),
    StructField("2000 Population", IntegerType(), True),
    StructField("2010 Population", IntegerType(), True)
])

# Create a SparkSession if it doesn't already exist
if "spark" not in locals():
    spark = SparkSession.builder \
        .appName("NYC Restaurant Inspection Pipeline") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

# Define paths
base_path = "/Users/hitesh/Documents/data-engineering/restraunt-inspection-pipeline"
raw_data_path = os.path.join(base_path, "raw_data")
processed_data_path = os.path.join(base_path, "processed_data")
output_data_path = os.path.join(base_path, "output_data")

# Create directories if they don't exist
for path in [raw_data_path, processed_data_path, output_data_path]:
    os.makedirs(path, exist_ok=True)

# File paths
restaurant_file = os.path.join(raw_data_path, "DOHMH_New_York_City_Restaurant_Inspection_Results_20250313.csv")
population_file = os.path.join(raw_data_path, "New_York_City_Population_By_Community_Districts_20250315.csv")

# CSV options
infer_schema = "false"
first_row_is_header = "true"
# Update CSV options
delimiter = ","

def load_data():
    """Load data from source files"""
    print("Loading restaurant inspection data...")
    restaurant_df = spark.read.format("csv") \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .schema(restaurant_schema) \
        .load(restaurant_file)
    
    print("\nLoading population data...")
    population_df = spark.read.format("csv") \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .schema(population_schema) \
        .load(population_file)
    
    return restaurant_df, population_df

def integrate_data(restaurant_df, population_df):
    """Integrate restaurant and population data"""
    print("Integrating datasets...")
    
    # First verify the columns exist
    print("Available columns in restaurant_df:", restaurant_df.columns)
    print("Available columns in population_df:", population_df.columns)
    
    # Map Community Board to CD Number for joining
    restaurant_df = restaurant_df.withColumnRenamed("Community Board", "CD_NUMBER")
    
    # Join datasets using correct column names
    joined_df = restaurant_df.join(
        population_df,
        (restaurant_df.BORO == population_df.BOROUGH) & 
        (restaurant_df.CD_NUMBER == population_df["CD Number"]),  # Use original column name
        "left"
    )
    
    return joined_df

def clean_restaurant_data(df):
    """Clean and transform restaurant inspection data"""
    print("Cleaning restaurant data...")
    
    # Convert date columns to proper date format
    df = df.withColumn("INSPECTION_DATE", F.to_date(F.col("INSPECTION DATE"), "MM/dd/yyyy"))
    df = df.withColumn("GRADE_DATE", F.to_date(F.col("GRADE DATE"), "MM/dd/yyyy"))
    df = df.withColumn("RECORD_DATE", F.to_date(F.col("RECORD DATE"), "MM/dd/yyyy"))
    
    # Drop rows with missing critical values
    df = df.dropna(subset=["CAMIS", "BORO", "ZIPCODE"])
    
    # Standardize borough names
    df = df.withColumn("BORO", F.upper(F.trim(F.col("BORO"))))
    
    # Create a full address column
    df = df.withColumn("FULL_ADDRESS", 
                      F.concat_ws(" ", F.col("BUILDING"), F.col("STREET"), 
                                 F.lit(", "), F.col("BORO"), F.lit(", NY "), F.col("ZIPCODE")))
    
    # Categorize violations
    df = df.withColumn("VIOLATION_CATEGORY", 
                      F.when(F.col("CRITICAL FLAG") == "Critical", "Critical")
                       .when(F.col("CRITICAL FLAG") == "Not Critical", "Non-Critical")
                       .otherwise("Unknown"))
    
    return df

def clean_population_data(df):
    """Clean and transform population data"""
    print("Cleaning population data...")
    
    # Standardize borough names
    df = df.withColumn("BOROUGH", F.upper(F.trim(F.col("Borough"))))
    
    # Calculate population density using 2010 data instead of 2020
    df = df.withColumn("POPULATION_DENSITY_2010", 
                      F.col("2010 Population") / 1.0)  # Replace with actual area
    
    # Calculate growth rate (2000-2010 instead of 2010-2020)
    df = df.withColumn("GROWTH_RATE_10YR", 
                      (F.col("2010 Population") - F.col("2000 Population")) / F.col("2000 Population"))
    
    return df

def create_analytical_views(df):
    """Create analytical views for different use cases"""
    print("Creating analytical views...")
    
    # 1. Demographics vs. Food Safety (using 2010 Population)
    food_safety_demo = df.groupBy("BOROUGH", "CD_NUMBER", "CD Name", "2010 Population") \
        .agg(
            F.count("CAMIS").alias("INSPECTION_COUNT"),
            F.avg("SCORE").alias("AVG_SCORE"),
            F.count(F.when(F.col("CRITICAL FLAG") == "Critical", True)).alias("CRITICAL_VIOLATIONS"),
            F.count(F.when(F.col("GRADE") == "A", True)).alias("GRADE_A_COUNT"),
            F.count(F.when(F.col("GRADE") == "B", True)).alias("GRADE_B_COUNT"),
            F.count(F.when(F.col("GRADE") == "C", True)).alias("GRADE_C_COUNT")
        )
    
    # 2. Cuisine & Hygiene Mapping
    cuisine_hygiene = df.groupBy("CUISINE DESCRIPTION") \
        .agg(
            F.count("CAMIS").alias("RESTAURANT_COUNT"),
            F.avg("SCORE").alias("AVG_SCORE"),
            F.count(F.when(F.col("CRITICAL FLAG") == "Critical", True)).alias("CRITICAL_VIOLATIONS"),
            F.count(F.when(F.col("GRADE") == "A", True)).alias("GRADE_A_COUNT"),
            F.count(F.when(F.col("GRADE") == "B", True)).alias("GRADE_B_COUNT"),
            F.count(F.when(F.col("GRADE") == "C", True)).alias("GRADE_C_COUNT")
        )
    
    # 3. Restaurant Location Intelligence (using 2010 Population)
    location_intel = df.groupBy("ZIPCODE", "BORO") \
        .agg(
            F.countDistinct("CAMIS").alias("RESTAURANT_COUNT"),
            F.avg("2010 Population").alias("AVG_POPULATION"),
            F.avg("POPULATION_DENSITY_2010").alias("AVG_POPULATION_DENSITY"),
            F.countDistinct("CUISINE DESCRIPTION").alias("CUISINE_VARIETY")
        )
    
    # 4. Violation Risk Prediction Dataset (using 2010 Population)
    violation_risk = df.select(
        "CAMIS", "DBA", "BORO", "ZIPCODE", "CUISINE DESCRIPTION", 
        "INSPECTION_DATE", "VIOLATION CODE", "VIOLATION DESCRIPTION", 
        "CRITICAL FLAG", "SCORE", "GRADE", "2010 Population", "GROWTH_RATE_10YR"
    )
    
    return {
        "food_safety_demo": food_safety_demo,
        "cuisine_hygiene": cuisine_hygiene,
        "location_intel": location_intel,
        "violation_risk": violation_risk
    }

def save_data(dataframes, timestamp):
    """Save processed data to output location"""
    print("Saving processed data...")
    
    # Create a timestamped folder for this run
    run_path = os.path.join(output_data_path, timestamp)
    os.makedirs(run_path, exist_ok=True)
    
    # Save each analytical view
    for name, df in dataframes.items():
        output_path = os.path.join(run_path, name)
        print(f"Saving {name} to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
    
    print(f"Data saved to {run_path}")

def main():
    """Main ETL pipeline execution"""
    print("Starting NYC Restaurant Inspection Pipeline...")
    
    # Generate timestamp for this run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Load data
        restaurant_df, population_df = load_data()
        
        # Clean data
        restaurant_df_clean = clean_restaurant_data(restaurant_df)
        population_df_clean = clean_population_data(population_df)
        
        # Save intermediate results
        restaurant_df_clean.write.mode("overwrite").parquet(os.path.join(processed_data_path, "restaurant_clean"))
        population_df_clean.write.mode("overwrite").parquet(os.path.join(processed_data_path, "population_clean"))
        
        # Integrate data
        integrated_df = integrate_data(restaurant_df_clean, population_df_clean)
        integrated_df.write.mode("overwrite").parquet(os.path.join(processed_data_path, "integrated_data"))
        
        # Create analytical views
        analytical_views = create_analytical_views(integrated_df)
        
        # Save results
        save_data(analytical_views, timestamp)
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in pipeline execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()