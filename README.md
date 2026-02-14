# MLDB ASSIGNMENT 1

A comprehensive text analytics project using Apache Spark to analyze the Project Gutenberg book collection. This project demonstrates distributed data processing, natural language processing, and network analysis techniques.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Dataset](#dataset)
- [Usage](#usage)

## Overview

This repo contains programs for hadoop and spark for MLDB Assignment 1, has following

1. **Word Count** - A map reduce method to count words in a text file
1. **Metadata Extraction** - Extracts and analyzes book metadata (title, author, year, language, encoding)
2. **TF-IDF Similarity** - Calculates term importance and identifies similar books using cosine similarity
3. **Author Influence Network** - Constructs temporal influence networks between authors

## Project Structure

```
project-gutenberg-analysis/
│
├── load.py                    # Initial data loading script
├── q10.py                     # Question 10: Metadata extraction and analysis
├── q11.py                     # Question 11: TF-IDF and book similarity
├── q12.py                     # Question 12: Author influence network
├── README.md                  # This file
├── WordCount.java             # Questions 1-9 : Word Count Using Hadoop
│
└── outputs/                   # Generated output files
    ├── books_dataset.parquet
    ├── metadata_extracted.parquet
    ├── temporal_distribution/
    ├── language_statistics/
    ├── q10_summary.txt
    ├── tfidf_vectors.parquet
    ├── similarity_analysis.txt
    ├── influence_network.parquet
    ├── author_metrics.parquet
    └── q12_summary.txt
```

## Prerequisites

### Required Software

- **Java 8 or higher** (required by Spark)
- **Python 3.7+**
- **Apache Spark 3.5.0**
- **Apache Hadoop 3.5.0**

### Python Dependencies

```bash
pip install pyspark numpy pyArrow
```

## Installation

Refer to [HADOOP_SPARK_SETUP.md](HADOOP_SPARK_SETUP.md) to setup hadoop and spark

### 2. Install Python Dependencies

```bash
pip install pyspark==3.5.0 numpy
```

### 3. Clone This Repository

```bash
git clone git@github.com:Abhijeetk431/MLBD_Assignment1.git
cd MLBD_Assignment1
```

## Dataset

### Project Gutenberg Collection

This project analyzes books from the **Project Gutenberg** collection, a library of over 70,000 free eBooks.

### Dataset Structure

Each book is stored as a `.txt` file with metadata in the header:

```
Title: [Book Title]
Author: [Author Name]
Release Date: [Date] [EBook #XXXXX]
Language: [Language]
Character set encoding: [Encoding]

*** START OF THE PROJECT GUTENBERG EBOOK [TITLE] ***
[Book content...]
*** END OF THE PROJECT GUTENBERG EBOOK [TITLE] ***
```

### Obtaining the Dataset

1. **Download from Project Gutenberg:**
   ```bash
   # Example: Download a collection
   wget -r -A.txt https://www.gutenberg.org/files/
   ```

2. **Copy to HDFS (Optional):**
   ```bash
   hadoop fs -copyFromLocal /path/to/books/* /user/hadoop/D184MB/
   hadoop fs -ls /user/hadoop/D184MB/
   ```

3. **Update Script Paths:**
   Edit `load.py` and set:
   ```python
   BOOKS_DIR = "/path/to/your/books/directory"
   ```

## Usage

### Word Count
Follow the [HADOOP_SPARK_SETUP.md](HADOOP_SPARK_SETUP.md) to setup hadoop and run minimal word count example
Move 200.txt from local filesystem to hadoop dfs and then run command
```bash
hadoop jar wordcount.jar WordCount /user/hadoop/200.txt /user/hadoop/output/exp
```
To run word count for 200.txt file

### Load Books into Spark

```bash
python load.py
```

**Output:** `books_dataset.parquet` (DataFrame with file_name and text columns)

---

### Extract and Analyze Metadata (Question 10)

```bash
python q10.py
```

**What It Does:**
- Extracts title, author, year, language, and encoding
- Calculates statistics on metadata completeness
- Analyzes distribution by year, language, and author
- Identifies top authors and encoding types

**Key Outputs:**
- `metadata_extracted.parquet` - Structured metadata
- `temporal_distribution/` - Books per year statistics
- `language_statistics/` - Language distribution
- `q10_summary.txt` - Summary report

---

### Calculate TF-IDF and Find Similar Books (Question 11)

```bash
python q11.py
```

**What It Does:**
- Preprocesses text (lowercase, remove punctuation, tokenize)
- Removes stop words
- Calculates TF-IDF scores for each word in each book
- Computes cosine similarity between books
- Identifies top 5 most similar books to a target book

**Key Outputs:**
- `tfidf_vectors.parquet` - TF-IDF vectors for all books
- `similarity_analysis.txt` - Similarity rankings

### Build Author Influence Network (Question 12)

```bash
python q12.py
```

**What It Does:**
- Extracts author and publication year
- Creates directed edges between authors based on temporal proximity
- Calculates in-degree (influenced by) and out-degree (influenced) metrics
- Identifies most influential and most influenced authors

**Key Outputs:**
- `influence_network.parquet` - Network edge list
- `author_metrics.parquet` - Degree metrics per author
- `q12_summary.txt` - Network summary

