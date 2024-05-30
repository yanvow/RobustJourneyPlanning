# Final Assignment: Robust Journey Planning

## HOW-TO

## Project Instructions

### 1. Timeline Data Preprocessing (timeline_data_preprocessing.py)

This script is designed to preprocess the timeline data. It sets the context for journey planning and discusses the importance of considering delays and connections.

*Instructions:*
- This file primarily provides a narrative context for the data processing and planning.
- Follow the theoretical framework and context described to understand how journey planning data is considered and why delays are significant.

### 2. Save CSV (save_csv.ipynb)

This script is used to save HDFS tables into CSV files.

*Instructions:*
1. *Set up the HDFS connection:*
   - Ensure you have the required environment variables set up: HIVE_SERVER, HADOOP_DEFAULT_FS, and USER.
   - If not set, you can manually specify these values within the script.

2. *Connect to Hive:*
   - The script uses the pyhive library to connect to Hive.
   - Create a cursor object to execute queries.

3. *Save tables to CSV:*
   - Use the cursor to execute SQL queries and fetch the data.
   - Convert the fetched data into a Pandas DataFrame.
   - Save the DataFrame to a CSV file using df.to_csv().

### 3. Delay Data Preprocessing (delay_data_preprocessing.ipynb)

This script preprocesses delay data, focusing on cleaning and analyzing datasets to develop a delay model.

*Instructions:*
1. *Import required libraries:*
   - Ensure you have the necessary libraries: pyspark, numpy, plotly, and pandas.

2. *Load the datasets:*
   - Load the istdaten and sbb_stops_lausanne_region datasets.
   - Clean the data by removing irrelevant entries and handling missing values.

3. *Analyze the data:*
   - Assess the importance of various parameters such as day of the week, time of day, type of transport, and station traffic.
   - Construct a DataFrame that captures delays based on selected parameters.

4. *Predict delays:*
   - Use the cleaned and analyzed data to build a model that predicts delays.
   - Calculate the probability of not missing a connection to ensure timely arrivals.

### 4. Main Notebook (main_notebook.ipynb)

This notebook creates a graph from the preprocessed timetable dataset.

*Instructions:*
1. *Import required libraries:*
   - Import necessary libraries such as pandas, networkx, matplotlib, and plotly.

2. *Load the dataset:*
   - Load the preprocessed timetable dataset (e.g., lausanne_pairs_df.csv).

3. *Process the data:*
   - Modify the route_desc column to categorize routes into 'Bus' and 'Zug' (train), keeping 'WALKING' unchanged.
   - Convert times into a suitable format (e.g., seconds).

4. *Create the graph:*
   - Use the NetworkX library to create a graph from the data.
   - Visualize the graph using Matplotlib or Plotly.

5. *Optimize routes:*
   - Implement algorithms to find the best routes considering delays and connection times.
   - Use widgets to interactively display and adjust route options.

By following these instructions, you can preprocess the data, save it into CSV files, analyze delay patterns, and create visualizations for optimal journey planning. If you have any specific questions or need further details on a particular step, feel free to ask!


## Problem Motivation

Imagine you are a regular user of the public transport system, and you are checking the operator's schedule to meet your friends for a class reunion. The choices are:

1. You could leave in 10mins, and arrive with enough time to spare for gossips before the reunion starts.

2. You could leave now on a different route and arrive just in time for the reunion.

Undoubtedly, if this is the only information available, most of us will opt for option 1.

If we now tell you that option 1 carries a fifty percent chance of missing a connection and be late for the reunion. Whereas, option 2 is almost guaranteed to take you there on time. Would you still consider option 1?

Probably not. However, most public transport applications will insist on the first option. This is because they are programmed to plan routes that offer the shortest travel times, without considering the risk factors.
