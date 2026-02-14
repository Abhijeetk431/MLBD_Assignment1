"""
Book Metadata Extraction and Analysis

Metadata extracted:
- Title
- Author
- Release date (with year extraction)
- Language
- Character encoding

Analysis performed:
- Books per year distribution
- Language distribution
- Top authors by book count
- Encoding types used
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def create_spark_session():
    """Initialize Spark session with optimized configuration."""
    return SparkSession.builder \
        .appName("Book Metadata Extraction") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def print_header(title, char="="):
    """Print a formatted header."""
    print("\n" + char * 80)
    print(f"  {title}")
    print(char * 80)


def extract_metadata(spark, input_path):
    """
    Extract metadata from books using regular expressions.
    
    Regular Expression Patterns:
    - Title: Matches 'Title: <content>' until newline
    - Author: Matches 'Author: <content>' until newline
    - Release Date: Matches 'Release Date: <content>' until newline or '['
    - Language: Matches 'Language: <content>' until newline
    - Year: Extracts 4-digit year from release date
    - Encoding: Matches 'Character set encoding: <content>' until newline
    
    Args:
        spark: Active SparkSession
        input_path: Path to books parquet file
        
    Returns:
        DataFrame with extracted metadata
    """
    print("\nLoading books and extracting metadata...")
    
    metadata_df = spark.read.parquet(input_path) \
        .withColumn("title", 
            F.regexp_extract(F.col("text"), r'Title:\s*(.+?)(?:\r?\n)', 1)) \
        .withColumn("author", 
            F.regexp_extract(F.col("text"), r'Author:\s*(.+?)(?:\r?\n)', 1)) \
        .withColumn("release_date", 
            F.regexp_extract(F.col("text"), r'Release Date:\s*(.+?)(?:\r?\n|\[)', 1)) \
        .withColumn("language", 
            F.regexp_extract(F.col("text"), r'Language:\s*(.+?)(?:\r?\n)', 1)) \
        .withColumn("year_str", 
            F.regexp_extract(F.col("release_date"), r'(\d{4})', 1)) \
        .withColumn("year", 
            F.when(F.col("year_str") != "", F.col("year_str").cast(IntegerType()))
             .otherwise(None)) \
        .withColumn("encoding", 
            F.regexp_extract(F.col("text"), r'Character set encoding:\s*(.+?)(?:\r?\n)', 1)) \
        .withColumn("title", F.when(F.col("title") == "", None).otherwise(F.col("title"))) \
        .withColumn("author", F.when(F.col("author") == "", None).otherwise(F.col("author"))) \
        .withColumn("language", F.when(F.col("language") == "", None).otherwise(F.col("language"))) \
        .withColumn("encoding", F.when(F.col("encoding") == "", None).otherwise(F.col("encoding"))) \
        .select("file_name", "title", "author", "year", "language", "encoding", "release_date") \
        .cache()
    
    # Trigger cache
    total_books = metadata_df.count()
    print(f"Metadata extracted from {total_books} books")
    
    return metadata_df


def compute_statistics(metadata_df):
    """
    Compute summary statistics for the metadata.
    
    Args:
        metadata_df: DataFrame with metadata
        
    Returns:
        Dictionary containing statistical summaries
    """
    print("\nComputing statistics...")
    
    stats = metadata_df.agg(
        F.count("*").alias("total_books"),
        F.count(F.when(F.col("title").isNotNull(), 1)).alias("with_title"),
        F.count(F.when(F.col("author").isNotNull(), 1)).alias("with_author"),
        F.count(F.when(F.col("year").isNotNull(), 1)).alias("with_year"),
        F.count(F.when(F.col("language").isNotNull(), 1)).alias("with_language"),
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year")
    ).collect()[0]
    
    print("Statistics computed")
    return stats


def analyze_yearly_distribution(metadata_df):
    """Analyze books per year."""
    return metadata_df \
        .filter(F.col("year").isNotNull()) \
        .groupBy("year") \
        .count() \
        .orderBy("year") \
        .cache()


def analyze_language_distribution(metadata_df):
    """Analyze language distribution."""
    return metadata_df \
        .filter(F.col("language").isNotNull()) \
        .groupBy("language") \
        .count() \
        .orderBy(F.desc("count")) \
        .cache()


def analyze_top_authors(metadata_df, limit=10):
    """Find top authors by book count."""
    return metadata_df \
        .filter(F.col("author").isNotNull()) \
        .groupBy("author") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(limit) \
        .cache()


def analyze_encoding_distribution(metadata_df):
    """Analyze encoding type distribution."""
    return metadata_df \
        .filter(F.col("encoding").isNotNull()) \
        .groupBy("encoding") \
        .count() \
        .orderBy(F.desc("count")) \
        .cache()


def display_results(stats, metadata_df, books_per_year, language_dist, 
                   top_authors, encoding_dist):
    """Display all analysis results."""
    
    print_header("METADATA QUALITY SUMMARY", "-")
    print(f"\n  Total books analyzed:     {stats['total_books']}")
    print(f"  Books with title:         {stats['with_title']:>6} ({100 * stats['with_title'] / stats['total_books']:.1f}%)")
    print(f"  Books with author:        {stats['with_author']:>6} ({100 * stats['with_author'] / stats['total_books']:.1f}%)")
    print(f"  Books with year:          {stats['with_year']:>6} ({100 * stats['with_year'] / stats['total_books']:.1f}%)")
    print(f"  Books with language:      {stats['with_language']:>6} ({100 * stats['with_language'] / stats['total_books']:.1f}%)")
    
    if stats["min_year"]:
        print(f"\n  Year range:               {stats['min_year']} - {stats['max_year']}")
    
    print_header("SAMPLE METADATA", "-")
    metadata_df.show(10, truncate=45)
    
    print_header("BOOKS PER YEAR (Top 20)", "-")
    books_per_year.show(20)
    
    print_header("LANGUAGE DISTRIBUTION", "-")
    language_dist.show(20, truncate=False)
    
    print_header("TOP 10 AUTHORS BY BOOK COUNT", "-")
    top_authors.show(10, truncate=False)
    
    print_header("ENCODING TYPES", "-")
    encoding_dist.show(10, truncate=False)


def save_results(metadata_df, stats, books_per_year, language_dist, top_authors):
    """Save all analysis results to disk."""
    
    print_header("SAVING RESULTS", "-")
    
    # Save metadata as parquet
    metadata_df.write.mode("overwrite").parquet("metadata_extracted.parquet")
    print("Saved: metadata_extracted.parquet")
    
    # Save CSVs for time series and language data
    books_per_year.write.mode("overwrite").option("header", "true").csv("temporal_distribution")
    print("Saved: temporal_distribution/")
    
    language_dist.write.mode("overwrite").option("header", "true").csv("language_statistics")
    print("Saved: language_statistics/")
    
    # Create text summary
    summary_lines = []
    summary_lines.append("=" * 80)
    summary_lines.append("BOOK METADATA EXTRACTION - SUMMARY REPORT")
    summary_lines.append("=" * 80)
    summary_lines.append("")
    summary_lines.append("DATASET STATISTICS:")
    summary_lines.append("-" * 80)
    summary_lines.append(f"Total books analyzed:     {stats['total_books']}")
    summary_lines.append(f"Books with title:         {stats['with_title']} ({100 * stats['with_title'] / stats['total_books']:.1f}%)")
    summary_lines.append(f"Books with author:        {stats['with_author']} ({100 * stats['with_author'] / stats['total_books']:.1f}%)")
    summary_lines.append(f"Books with year:          {stats['with_year']} ({100 * stats['with_year'] / stats['total_books']:.1f}%)")
    summary_lines.append(f"Books with language:      {stats['with_language']} ({100 * stats['with_language'] / stats['total_books']:.1f}%)")
    summary_lines.append("")
    
    if stats["min_year"]:
        summary_lines.append(f"Year range:               {stats['min_year']} - {stats['max_year']}")
        summary_lines.append("")
    
    summary_lines.append("TOP 5 LANGUAGES:")
    summary_lines.append("-" * 80)
    for row in language_dist.limit(5).collect():
        summary_lines.append(f"  {row['language']:<30} {row['count']:>6} books")
    
    summary_lines.append("")
    summary_lines.append("TOP 5 AUTHORS:")
    summary_lines.append("-" * 80)
    for row in top_authors.limit(5).collect():
        summary_lines.append(f"  {row['author']:<50} {row['count']:>3} books")
    
    summary_lines.append("")
    summary_lines.append("=" * 80)
    summary_lines.append("REGEX PATTERNS USED:")
    summary_lines.append("-" * 80)
    summary_lines.append("Title:        r'Title:\\s*(.+?)(?:\\r?\\n)'")
    summary_lines.append("Author:       r'Author:\\s*(.+?)(?:\\r?\\n)'")
    summary_lines.append("Release Date: r'Release Date:\\s*(.+?)(?:\\r?\\n|\\[)'")
    summary_lines.append("Language:     r'Language:\\s*(.+?)(?:\\r?\\n)'")
    summary_lines.append("Year:         r'(\\d{4})' (from release date)")
    summary_lines.append("Encoding:     r'Character set encoding:\\s*(.+?)(?:\\r?\\n)'")
    summary_lines.append("")
    summary_lines.append("=" * 80)
    
    with open("q10_summary.txt", "w") as f:
        f.write("\n".join(summary_lines))
    
    print("Saved: q10_summary.txt")


def cleanup(dataframes):
    """Unpersist all cached DataFrames."""
    for df in dataframes:
        df.unpersist()


def main():
    """Main execution function."""
    
    # Configuration
    INPUT_PATH = "books_dataset.parquet"
    
    print_header("BOOK METADATA EXTRACTION AND ANALYSIS")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Step 1: Extract metadata
        metadata_df = extract_metadata(spark, INPUT_PATH)
        
        # Step 2: Compute statistics
        stats = compute_statistics(metadata_df)
        
        # Step 3: Perform analyses
        print("\nPerforming analyses...")
        books_per_year = analyze_yearly_distribution(metadata_df)
        language_dist = analyze_language_distribution(metadata_df)
        top_authors = analyze_top_authors(metadata_df)
        encoding_dist = analyze_encoding_distribution(metadata_df)
        print("All analyses complete")
        
        # Step 4: Display results
        display_results(stats, metadata_df, books_per_year, language_dist, 
                       top_authors, encoding_dist)
        
        # Step 5: Save results
        save_results(metadata_df, stats, books_per_year, language_dist, top_authors)
        
        # Cleanup
        cleanup([metadata_df, books_per_year, language_dist, top_authors, encoding_dist])
        
        print_header("QUESTION 10 COMPLETE")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
