"""
Loads all books from the Project Gutenberg dataset into a Spark DataFrame.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, collect_list, concat_ws


def create_spark_session():
    """Initialize Spark session with optimized configuration."""
    return SparkSession.builder \
        .appName("Project Gutenberg") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()


def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def load_books(spark, books_directory):
    """
    Load all book text files from the specified directory.
    
    Args:
        spark: Active SparkSession
        books_directory: Path to directory containing .txt files
        
    Returns:
        DataFrame with columns: file_name, text
    """
    print("\nLoading books from directory...")
    print(f"Source: {books_directory}")
    
    # Read all .txt files and extract filename
    books_df = spark.read.text(f"{books_directory}/*.txt") \
        .withColumn("full_path", input_file_name()) \
        .withColumn("file_name", regexp_extract("full_path", r'([^/]+\.txt)$', 1)) \
        .select("file_name", "value") \
        .withColumnRenamed("value", "text") \
        .filter("text != ''")
    
    # Combine all lines from each file into a single text column
    print("Aggregating text by file...")
    books_df = books_df.groupBy("file_name") \
        .agg(concat_ws("\n", collect_list("text")).alias("text"))
    
    return books_df


def save_books(books_df, output_path):
    """
    Save the books DataFrame to parquet format.
    
    Args:
        books_df: DataFrame to save
        output_path: Path for output parquet file
    """
    print(f"\nSaving to: {output_path}")
    books_df.write.parquet(output_path, mode="overwrite")
    print("Save complete.")


def display_sample(books_df):
    """Display sample book filenames."""
    print("\nSample of loaded books:")
    books_df.select("file_name").show(20, truncate=False)


def main():
    """Main execution function."""
    # Configuration
    BOOKS_DIR = "/home/hadoop/D184MB"
    OUTPUT_PATH = "books_dataset.parquet"
    
    print_header("PROJECT GUTENBERG BOOK LOADER")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load books
        books_df = load_books(spark, BOOKS_DIR)
        
        # Count and display
        total_books = books_df.count()
        print(f"\nTotal books loaded: {total_books}")
        
        display_sample(books_df)
        
        # Save results
        save_books(books_df, OUTPUT_PATH)
        
        print_header("LOADING COMPLETE")
        print(f"Successfully loaded and saved {total_books} books")
        print(f"Output location: {OUTPUT_PATH}\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
