# Lending Club Loan Data Cleaning and Transformation Pipeline using PySpark

## 🚀 Project Title:
**Lending Club Loan Data Engineering Project using PySpark**

## 📝 Description:
This Big Data project demonstrates a complete end-to-end data engineering pipeline built using Apache Spark (PySpark).
The goal is to process, clean, and transform a 1.7 GB Lending Club dataset consisting of more than 2 million loan records to prepare it for downstream analytics and ML.

## 📁 Dataset:
- **Source:** Lending Club Public Dataset
- **Format:** CSV
- **Size:** ~1.7 GB
- **Records:** 2+ Million

## 🔧 Technologies Used:
- Apache Spark (PySpark)
- HDFS (Hadoop Distributed File System)
- Hive (for table storage - optional)
- Linux-based cluster setup

## 🧪 Project Steps:

### 1. Spark Setup
- SparkSession initialization
- Dependencies imported

### 2. Data Loading
- Read the raw CSV file using `.option("header", True).csv(...)`
- Schema printed and basic records previewed

### 3. Feature Selection
- Selected 35 relevant columns (loan, credit, and borrower-related)

### 4. Data Cleaning
- Dropped rows with nulls in critical columns (`loan_amnt`, `annual_inc`)
- Replaced % symbols and casted columns to appropriate data types
- Removed duplicate rows based on `id`

### 5. Data Transformation
- Created a custom metric called `loan_score = (loan_amnt / annual_inc) * 100`
- Generated a unique `uid` using MD5 hash of `id + issue_d`

### 6. Final Output
- Reordered and selected important final columns
- Printed transformed data using `.show()`
- Optional: Saved to Hive table `lending_club_cleaned`

