# Big Data Management - Academic Year 2024-2025

Corresponding README in greek can be found [here](https://github.com/manouslinard/bigdata-ntuamsc/blob/main/README_GR.md).

## Project Description

This repository contains the implementation of the semester project for the "Big Data Management" course of the MSc in Data Science and Machine Learning at NTUA. The project involves analyzing datasets related to crimes in Los Angeles, using Apache Hadoop and Apache Spark.

## Repository Structure

The repository is organized according to the tasks of the project:

### Task 2 - question2
Conversion of the original data and storing them in parquet format on HDFS.

### Task 3 - question3
Implementation of Query 1 using RDD and DataFrame APIs (with and without udf), which involves ranking age groups of victims in incidents of aggravated assault.

### Task 4 - question4
Implementation of Query 2 using RDD, DataFrame, and SQL APIs, which involves finding the top 3 Police Departments with the highest percentage of closed cases for each year.

### Task 5 - question5
Implementation of Query 3 using RDD and DataFrame APIs, which involves calculating the average annual income per person for each ZipCode in Los Angeles.

### Task 6 - question6
Implementation of Query 4 using DataFrame API, which involves calculating the number of weapon-related crimes per police department and the average distance from incidents.

## Technologies

- Apache Hadoop (version ≥ 3.0)
- Apache Spark (version ≥ 3.5)
- Python

## Code Execution

To execute the code, access to the laboratory infrastructure provided for the course is required. After connecting to the [cslab](http://www.cslab.ntua.gr/) VPN of NTUA, and setting up based on the guides in the course [repository](https://github.com/ikons/bigdata-dsml), you need to upload the files of this repository to HDFS by running the following (assuming the repository was cloned locally to the home directory of a UNIX system):

```
hadoop fs -put ~/bigdata-ntuamsc/ assignment_code
```

To execute a specific python file, run the command:

```
spark-submit hdfs://hdfs-namenode:9000/user/<username>/assignment_code/<path_to_file>
```

where \<username\> is replaced with the username and \<path_to_file\> is the path to the file to be executed in the laboratory infrastructure.

## Author

Manousos Linardakis, AM: 03400256

Email: manousoslinardakis@mail.ntua.gr
