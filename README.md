# Student Records Management and Analytics
This project demonstrates student data management and analysis using **MongoDB** and **Apache Spark SQL** with Scala.
## Technologies Used
- Scala
- MongoDB
- Apache Spark SQL
- SBT (Scala Build Tool)

## Project Features
### 1. MongoDB
- Import student records from CSV and JSON files.
- Store and manage student data in MongoDB.
- Perform CRUD operations (Create, Read, Update, Delete).
- Create indexes to improve query performance.
- Execute queries with filtering and sorting.
- Perform aggregation operations.
- Analyze query execution plans using `explain()` to examine index usage.

### 2. Apache Spark SQL
- Load student data from JSON files.
- Define a custom schema for the dataset.
- Perform data analysis using Spark SQL and DataFrame operations.
- Calculate statistics such as average GPA and course-related statistics.
- Save analytical results in Parquet format.

## Project Structure
Student-Records-MongoDB-SparkSQL/
│
├── src/
│   └── main/
│       └── scala/
│           ├── MongoApp.scala
│           └── SparkApp.scala
│
├── project/
│   └── build.properties
│
├── students.csv
├── students.json
└── build.sbt

## How to Run
Run the MongoDB application:
```bash
sbt "runMain MongoApp"
```
Run the Spark application:
```bash
sbt "runMain SparkApp"
```

## Purpose
The goal of this project is to practice working with NoSQL databases and big data processing tools, focusing on data storage, querying, indexing, aggregation, and analytics.
