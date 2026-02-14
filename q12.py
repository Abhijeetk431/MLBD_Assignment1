"""
Author Influence Network Analysis

Constructs and analyzes an author influence network based on temporal
proximity of book publications.

Network Construction:
- Authors are nodes
- Directed edge from Author A to Author B if:
  * A's book was published before B's book
  * Time difference ≤ X years (configurable window)

Metrics Calculated:
- Out-degree: Number of authors potentially influenced (successors)
- In-degree: Number of authors who potentially influenced (predecessors)

Limitations:
- Simplified model: True influence involves citations, themes, style, etc.
- Temporal proximity is a weak proxy for actual influence
- Does not account for:
  * Author awareness/reading habits
  * Genre boundaries
  * Cultural/linguistic barriers
  * Posthumous influence

Representation Choice:
- DataFrame approach: Better for aggregations and SQL-like operations
- RDD approach: More flexible for complex graph algorithms
- GraphFrames/GraphX: Specialized graph libraries for advanced analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def create_spark_session():
    """Initialize Spark session with optimized configuration."""
    return SparkSession.builder \
        .appName("Q12: Author Influence Network") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def print_header(title, char="="):
    """Print a formatted header."""
    print("\n" + char * 80)
    print(f"  {title}")
    print(char * 80)


def extract_author_metadata(spark, input_path):
    """
    Extract author and publication year from books.
    
    Uses regular expressions to extract:
    - Author: Matches 'Author: <name>' pattern
    - Year: Extracts 4-digit year from 'Release Date:' field
    
    Filters to keep only books with valid author and year data.
    
    Args:
        spark: Active SparkSession
        input_path: Path to books parquet file
        
    Returns:
        DataFrame with columns: file_name, author, year
    """
    print("\nExtracting author and publication metadata...")
    
    metadata_df = spark.read.parquet(input_path) \
        .select("file_name", "text") \
        .withColumn("author", 
            F.regexp_extract(F.col("text"), r'Author:\s*(.+?)(?:\r?\n)', 1)) \
        .withColumn("year_str", 
            F.regexp_extract(F.col("text"), r'Release Date:.*?(\d{4})', 1)) \
        .withColumn("year", 
            F.when(F.col("year_str") != "", F.col("year_str").cast(IntegerType()))
             .otherwise(None)) \
        .filter((F.col("author") != "") & F.col("year").isNotNull()) \
        .select("file_name", "author", "year") \
        .distinct() \
        .cache()
    
    books_count = metadata_df.count()
    print(f"Found {books_count} books with valid author and year")
    
    return metadata_df


def build_influence_network(metadata_df, time_window=10):
    """
    Construct directed influence network based on publication timing.
    
    Network Construction Logic:
    - Create edge from Author A to Author B if:
      1. A and B are different authors
      2. A published before B (year_A < year_B)
      3. Time difference ≤ time_window years
    
    This is a SIMPLIFIED model. True influence networks would require:
    - Citation analysis
    - Thematic similarity
    - Explicit acknowledgments
    - Genre/period context
    
    Representation:
    - Uses DataFrame for efficient SQL-like operations
    - Self-join to compare all author pairs
    - Filters for temporal constraints
    
    Scalability:
    - Current approach: O(N²) self-join
    - For millions of books:
      * Use broadcast joins for smaller dimensions
      * Partition by year ranges
      * Consider approximate methods
      * Use GraphX/GraphFrames for advanced algorithms
    
    Args:
        metadata_df: DataFrame with author and year
        time_window: Maximum years between publications for influence edge
        
    Returns:
        DataFrame with columns: influencer, influenced
    """
    print_header("BUILDING INFLUENCE NETWORK", "-")
    print(f"Time window: {time_window} years")
    print(f"Creating edges via self-join...")
    
    # Self-join to create all potential influence pairs
    books_a = metadata_df.alias("a")
    books_b = metadata_df.alias("b")
    
    influence_edges = books_a.join(
        books_b,
        (F.col("a.author") != F.col("b.author")) &
        (F.col("a.year") < F.col("b.year")) &
        (F.col("b.year") - F.col("a.year") <= time_window)
    ) \
        .select(
            F.col("a.author").alias("influencer"),
            F.col("b.author").alias("influenced")
        ) \
        .distinct() \
        .cache()
    
    total_edges = influence_edges.count()
    print(f"Created {total_edges:,} influence edges")
    
    return influence_edges


def compute_network_metrics(metadata_df, influence_edges):
    """
    Calculate in-degree and out-degree for all authors.
    
    Metrics:
    - Out-degree: Number of authors this author potentially influenced
      (number of edges where this author is the source)
    
    - In-degree: Number of authors who potentially influenced this author
      (number of edges where this author is the target)
    
    High out-degree = "influential" (many successors)
    High in-degree = "influenced" (many predecessors)
    
    Args:
        metadata_df: DataFrame with all authors
        influence_edges: DataFrame with influence relationships
        
    Returns:
        DataFrame with columns: author, in_degree, out_degree
    """
    print_header("COMPUTING NETWORK METRICS", "-")
    print("Calculating degrees...")
    
    # Out-degree: count outgoing edges (authors influenced)
    out_degree = influence_edges.groupBy("influencer").count() \
        .withColumnRenamed("count", "out_degree") \
        .withColumnRenamed("influencer", "author")
    
    # In-degree: count incoming edges (authors who influenced)
    in_degree = influence_edges.groupBy("influenced").count() \
        .withColumnRenamed("count", "in_degree") \
        .withColumnRenamed("influenced", "author")
    
    # Get all unique authors
    all_authors = metadata_df.select("author").distinct()
    
    # Join all metrics (left join to include authors with degree 0)
    author_metrics = all_authors \
        .join(in_degree, "author", "left") \
        .join(out_degree, "author", "left") \
        .fillna(0) \
        .cache()
    
    print("Metrics computed for all authors")
    return author_metrics


def display_results(metadata_df, influence_edges, author_metrics, time_window):
    """Display network analysis results."""
    
    print_header("NETWORK SUMMARY", "-")
    
    books_count = metadata_df.count()
    authors_count = metadata_df.select("author").distinct().count()
    edges_count = influence_edges.count()
    
    print(f"\n  Books analyzed:           {books_count:>8}")
    print(f"  Unique authors:           {authors_count:>8}")
    print(f"  Time window:              {time_window:>8} years")
    print(f"  Influence edges:          {edges_count:>8}")
    
    if authors_count > 0:
        avg_out = author_metrics.agg(F.avg("out_degree")).collect()[0][0]
        avg_in = author_metrics.agg(F.avg("in_degree")).collect()[0][0]
        print(f"  Average out-degree:       {avg_out:>8.2f}")
        print(f"  Average in-degree:        {avg_in:>8.2f}")
    
    print_header("TOP 5 MOST INFLUENTIAL AUTHORS (Highest Out-Degree)", "-")
    print("\n  These authors potentially influenced the most successors")
    print("  (based on temporal proximity of publications)\n")
    author_metrics.orderBy(F.desc("out_degree")).show(5, truncate=False)
    
    print_header("TOP 5 MOST INFLUENCED AUTHORS (Highest In-Degree)", "-")
    print("\n  These authors were potentially influenced by the most predecessors")
    print("  (based on temporal proximity of publications)\n")
    author_metrics.orderBy(F.desc("in_degree")).show(5, truncate=False)
    
    print_header("SAMPLE METADATA", "-")
    metadata_df.show(10, truncate=False)


def save_results(metadata_df, influence_edges, author_metrics, time_window):
    """Save all network analysis results."""
    
    print_header("SAVING RESULTS", "-")
    
    # Save network edges
    influence_edges.write.mode("overwrite").parquet("influence_network.parquet")
    print("Saved: influence_network.parquet")
    
    # Save author metrics
    author_metrics.write.mode("overwrite").parquet("author_metrics.parquet")
    print("Saved: author_metrics.parquet")
    
    # Create comprehensive summary
    books_count = metadata_df.count()
    authors_count = metadata_df.select("author").distinct().count()
    edges_count = influence_edges.count()
    
    with open("q12_summary.txt", "w") as f:
        f.write("=" * 80 + "\n")
        f.write("AUTHOR INFLUENCE NETWORK ANALYSIS\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("NETWORK CONFIGURATION:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Books with valid metadata:    {books_count}\n")
        f.write(f"Unique authors:               {authors_count}\n")
        f.write(f"Time window:                  {time_window} years\n")
        f.write(f"Total influence edges:        {edges_count}\n\n")
        
        f.write("METHODOLOGY:\n")
        f.write("-" * 80 + "\n")
        f.write("Extract author and publication year from each book\n")
        f.write("Create directed edge from Author A to Author B if:\n")
        f.write(f"   • A published before B (year_A < year_B)\n")
        f.write(f"   • Time difference ≤ {time_window} years\n")
        f.write("Calculate in-degree and out-degree for each author\n\n")
        
        f.write("METRICS EXPLANATION:\n")
        f.write("-" * 80 + "\n")
        f.write("Out-degree: Number of authors potentially influenced (successors)\n")
        f.write("In-degree:  Number of authors who potentially influenced (predecessors)\n\n")
        
        f.write("TOP 5 MOST INFLUENTIAL AUTHORS (Highest Out-Degree):\n")
        f.write("-" * 80 + "\n")
        for i, row in enumerate(author_metrics.orderBy(F.desc("out_degree")).limit(5).collect(), 1):
            f.write(f"{i}. {row['author']:<50} Out-degree: {row['out_degree']}\n")
        
        f.write("\nTOP 5 MOST INFLUENCED AUTHORS (Highest In-Degree):\n")
        f.write("-" * 80 + "\n")
        for i, row in enumerate(author_metrics.orderBy(F.desc("in_degree")).limit(5).collect(), 1):
            f.write(f"{i}. {row['author']:<50} In-degree: {row['in_degree']}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("LIMITATIONS AND CONSIDERATIONS:\n")
        f.write("-" * 80 + "\n")
        f.write("SIMPLIFIED MODEL:\n")
        f.write("   • True influence requires citation analysis, thematic similarity\n")
        f.write("   • Temporal proximity is a weak proxy for actual influence\n\n")
        f.write("TIME WINDOW EFFECTS:\n")
        f.write(f"   • Current window: {time_window} years\n")
        f.write("   • Smaller window: Fewer edges, more precise temporal influence\n")
        f.write("   • Larger window: More edges, captures longer-term trends\n\n")
        f.write("DATA REPRESENTATION:\n")
        f.write("   • Using DataFrames for SQL-like operations and aggregations\n")
        f.write("   • Alternative: GraphX/GraphFrames for advanced graph algorithms\n")
        f.write("   • Trade-off: Ease of use vs. specialized graph capabilities\n\n")
        f.write("SCALABILITY:\n")
        f.write("   • Current approach: O(N²) self-join\n")
        f.write("   • For millions of books: Use partitioning, broadcast joins\n")
        f.write("   • Consider approximate methods or sampling for massive datasets\n")
        f.write("=" * 80 + "\n")
    
    print("Saved: q12_summary.txt")


def cleanup(dataframes):
    """Unpersist all cached DataFrames."""
    for df in dataframes:
        df.unpersist()


def main():
    """Main execution function."""
    
    # Configuration
    INPUT_PATH = "books_dataset.parquet"
    TIME_WINDOW = 10  # years
    
    print_header("AUTHOR INFLUENCE NETWORK ANALYSIS")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Extract metadata
        metadata_df = extract_author_metadata(spark, INPUT_PATH)
        
        # Build influence network
        influence_edges = build_influence_network(metadata_df, TIME_WINDOW)
        
        # Compute metrics
        author_metrics = compute_network_metrics(metadata_df, influence_edges)
        
        # Display results
        display_results(metadata_df, influence_edges, author_metrics, TIME_WINDOW)
        
        # Save results
        save_results(metadata_df, influence_edges, author_metrics, TIME_WINDOW)
        
        # Cleanup
        cleanup([metadata_df, influence_edges, author_metrics])
        
        print_header("QUESTION 12 COMPLETE")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
