"""
TF-IDF and Book Similarity Analysis

Calculates TF-IDF scores for words in books and determines book similarity
using cosine similarity.

Process:
1. Text preprocessing (cleaning, tokenization, stop word removal)
2. TF-IDF calculation (Term Frequency × Inverse Document Frequency)
3. Cosine similarity computation between book vectors
4. Identification of most similar books

TF-IDF measures word importance by balancing:
- TF (Term Frequency): How often a word appears in a document
- IDF (Inverse Document Frequency): How rare the word is across all documents

Cosine similarity measures the angle between two vectors, ideal for
comparing documents regardless of their length.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, Tokenizer
import numpy as np


def create_spark_session():
    """Initialize Spark session with optimized configuration for 50 books."""
    return SparkSession.builder \
        .appName("Q11: TF-IDF and Book Similarity") \
        .master("local[4]") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def print_header(title, char="="):
    """Print a formatted header."""
    print("\n" + char * 80)
    print(f"  {title}")
    print(char * 80)


def load_books(spark, input_path, num_books=50):
    """
    Load a subset of books for analysis.
    
    Args:
        spark: Active SparkSession
        input_path: Path to books parquet file
        num_books: Number of books to analyze
        
    Returns:
        DataFrame with file_name and text columns
    """
    print(f"\nLoading {num_books} books...")
    
    books_df = spark.read.parquet(input_path) \
        .select("file_name", "text") \
        .limit(num_books) \
        .repartition(4)
    
    print(f"Loaded {num_books} books")
    return books_df


def preprocess_text(books_df):
    """
    Clean and preprocess book text.
    
    Steps:
    1. Convert to lowercase
    2. Remove punctuation and special characters
    3. Tokenize into words
    4. Remove stop words
    
    Args:
        books_df: DataFrame with raw text
        
    Returns:
        DataFrame with filtered words
    """
    print_header("TEXT PREPROCESSING", "-")
    
    # Clean text: lowercase + remove punctuation
    print("Cleaning text (lowercase, remove punctuation)...")
    cleaned_df = books_df.withColumn(
        "clean_text",
        F.lower(F.regexp_replace(F.col("text"), "[^a-zA-Z\\s]+", " "))
    )
    
    # Tokenize
    print("Tokenizing into words...")
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    tokenized_df = tokenizer.transform(cleaned_df)
    
    # Remove stop words
    print("Removing stop words...")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(tokenized_df) \
        .select("file_name", "filtered_words")
    
    print("Preprocessing complete")
    return filtered_df


def calculate_tfidf(filtered_df, num_features=2000):
    """
    Calculate TF-IDF features for each book.
    
    TF (Term Frequency): Measures how frequently a term appears in a document.
        TF = (Number of times term appears in document) / (Total terms in document)
    
    IDF (Inverse Document Frequency): Measures how rare a term is across all documents.
        IDF = log(Total documents / Documents containing term)
    
    TF-IDF = TF × IDF
    
    Why TF-IDF?
    - Highlights words that are important to a specific document
    - Reduces weight of common words (like "the", "is")
    - Increases weight of distinctive words
    
    Args:
        filtered_df: DataFrame with filtered words
        num_features: Size of feature vector (hash space)
        
    Returns:
        DataFrame with TF-IDF feature vectors
    """
    print_header("TF-IDF CALCULATION", "-")
    
    # Calculate Term Frequency using feature hashing
    print(f"Computing Term Frequency (hash space: {num_features})...")
    hashing_tf = HashingTF(
        inputCol="filtered_words", 
        outputCol="tf_features", 
        numFeatures=num_features
    )
    tf_df = hashing_tf.transform(filtered_df)
    
    # Calculate Inverse Document Frequency
    print("Computing Inverse Document Frequency...")
    idf = IDF(inputCol="tf_features", outputCol="tfidf_features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df) \
        .select("file_name", "tfidf_features")
    
    print("TF-IDF calculation complete")
    return tfidf_df


def compute_cosine_similarity(vec1, vec2_sparse):
    """
    Calculate cosine similarity between two vectors.
    
    Cosine Similarity = (A · B) / (||A|| × ||B||)
    
    Where:
    - A · B is the dot product of vectors A and B
    - ||A|| and ||B|| are the magnitudes (lengths) of the vectors
    
    Why Cosine Similarity?
    - Measures the angle between vectors, not their magnitude
    - Invariant to document length
    - Range: -1 to 1 (0 to 1 for positive vectors like TF-IDF)
    - Value of 1 means identical direction (most similar)
    - Value of 0 means orthogonal (no similarity)
    
    Args:
        vec1: First vector (numpy array)
        vec2_sparse: Second vector (sparse vector)
        
    Returns:
        Cosine similarity score (float)
    """
    vec2 = vec2_sparse.toArray()
    dot_product = np.dot(vec1, vec2)
    norm_product = np.linalg.norm(vec1) * np.linalg.norm(vec2)
    return float(dot_product / norm_product) if norm_product > 0 else 0.0


def find_similar_books(tfidf_df, target_index=0, top_k=5):
    """
    Find the most similar books to a target book.
    
    Scalability Note:
    For N documents, pairwise similarity requires Nx(N-1)/2 comparisons.
    - 50 books: ~1,225 comparisons (feasible in memory)
    - 1,000 books: ~500,000 comparisons (still manageable)
    - 100,000 books: ~5 billion comparisons (requires distributed approach)
    
    Spark can help scale through:
    - Distributed vector storage (RDD/DataFrame partitioning)
    - Approximate methods (LSH - Locality Sensitive Hashing)
    - Cartesian join with filtering
    - Matrix multiplication libraries (distributed BLAS)
    
    Args:
        tfidf_df: DataFrame with TF-IDF features
        target_index: Index of target book
        top_k: Number of similar books to return
        
    Returns:
        List of tuples: (book_name, similarity_score)
    """
    print_header("SIMILARITY CALCULATION", "-")
    
    # Collect all vectors (efficient for small dataset)
    print("Collecting book vectors...")
    book_vectors = tfidf_df.collect()
    
    # Extract target book
    target_book = book_vectors[target_index]["file_name"]
    target_vector = book_vectors[target_index]["tfidf_features"].toArray()
    
    print(f"Target book: {target_book}")
    print(f"Computing cosine similarities for {len(book_vectors) - 1} books...")
    
    # Compute similarities (vectorized)
    similarities = [
        (book["file_name"], compute_cosine_similarity(target_vector, book["tfidf_features"]))
        for book in book_vectors
        if book["file_name"] != target_book
    ]
    
    # Sort and get top K
    similarities.sort(key=lambda x: x[1], reverse=True)
    top_similar = similarities[:top_k]
    
    print(f"Found top {top_k} similar books")
    return target_book, top_similar


def display_results(target_book, top_similar):
    """Display similarity results."""
    
    print_header(f"TOP {len(top_similar)} MOST SIMILAR BOOKS", "-")
    print(f"\n  Target Book: {target_book}\n")
    print("  Rank  Similarity  Book")
    print("  " + "-" * 76)
    
    for rank, (book_name, similarity_score) in enumerate(top_similar, 1):
        print(f"  {rank:>2}.    {similarity_score:>6.4f}     {book_name}")


def save_results(tfidf_df, target_book, top_similar, num_features):
    """Save analysis results to disk."""
    
    print_header("SAVING RESULTS", "-")
    
    # Save TF-IDF features
    tfidf_df.write.mode("overwrite").parquet("tfidf_vectors.parquet")
    print("Saved: tfidf_vectors.parquet")
    
    # Save similarity results as text
    with open("similarity_analysis.txt", "w") as f:
        f.write("=" * 80 + "\n")
        f.write("TF-IDF AND BOOK SIMILARITY ANALYSIS\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("CONFIGURATION:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Dataset size:         50 books\n")
        f.write(f"TF-IDF features:      {num_features}\n")
        f.write(f"Target book:          {target_book}\n")
        f.write(f"Similarity metric:    Cosine Similarity\n\n")
        
        f.write("METHODOLOGY:\n")
        f.write("-" * 80 + "\n")
        f.write("1. Text preprocessing: lowercase, remove punctuation, tokenize\n")
        f.write("2. Stop word removal using Spark's default English stop words\n")
        f.write("3. TF-IDF calculation using HashingTF and IDF\n")
        f.write("4. Cosine similarity computation between all book pairs\n\n")
        
        f.write(f"TOP {len(top_similar)} MOST SIMILAR BOOKS:\n")
        f.write("-" * 80 + "\n")
        for rank, (book_name, similarity_score) in enumerate(top_similar, 1):
            f.write(f"{rank:2}. {book_name:40} - Similarity: {similarity_score:.4f}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("EXPLANATION:\n")
        f.write("-" * 80 + "\n")
        f.write("TF-IDF (Term Frequency-Inverse Document Frequency) weights words by:\n")
        f.write("  • TF: How often a word appears in a document\n")
        f.write("  • IDF: How rare the word is across all documents\n")
        f.write("  • TF-IDF = TF × IDF\n\n")
        f.write("Cosine Similarity measures the angle between document vectors:\n")
        f.write("  • Range: 0 to 1 (for positive vectors)\n")
        f.write("  • 1 = identical direction (most similar)\n")
        f.write("  • 0 = orthogonal (no similarity)\n")
        f.write("  • Invariant to document length\n")
        f.write("=" * 80 + "\n")
    
    print("Saved: similarity_analysis.txt")


def main():
    """Main execution function."""
    
    # Configuration
    INPUT_PATH = "books_dataset.parquet"
    NUM_BOOKS = 50
    NUM_FEATURES = 2000
    TOP_K = 5
    
    print_header("TF-IDF AND BOOK SIMILARITY ANALYSIS")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Step 1: Load books
        books_df = load_books(spark, INPUT_PATH, NUM_BOOKS)
        
        # Step 2: Preprocess text
        filtered_df = preprocess_text(books_df)
        
        # Step 3: Calculate TF-IDF
        tfidf_df = calculate_tfidf(filtered_df, NUM_FEATURES)
        
        # Step 4: Find similar books
        target_book, top_similar = find_similar_books(tfidf_df, top_k=TOP_K)
        
        # Step 5: Display results
        display_results(target_book, top_similar)
        
        # Step 6: Save results
        save_results(tfidf_df, target_book, top_similar, NUM_FEATURES)
        
        print_header("QUESTION 11 COMPLETE")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
