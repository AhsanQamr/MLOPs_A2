# etl_script.py
import pandas as pd
import os
from datetime import datetime

# Data extraction
def extract_data():
    # Replace this with your actual data extraction logic
    # Example: Reading a CSV file for demonstration purposes
    data = pd.read_csv('source_data.csv')
    return data

# Data transformation
def transform_data(data):
    # Replace this with your actual data transformation logic
    # Example: Dropping null values for demonstration purposes
    transformed_data = data.dropna()
    return transformed_data

# Data loading
def load_data(transformed_data):
    # Replace this with your actual data loading logic
    # Example: Saving the transformed data to a CSV file with timestamp
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file = f'output_data_{timestamp_str}.csv'
    transformed_data.to_csv(output_file, index=False)
    return output_file

if __name__ == "__main__":
    # ETL process
    data = extract_data()
    transformed_data = transform_data(data)
    output_file = load_data(transformed_data)
    
    print("ETL process completed successfully.")
    print(f"Transformed data saved to: {output_file}")
