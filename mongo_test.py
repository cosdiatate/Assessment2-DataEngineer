from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")

# Select database
db = client["gdba707_partb"]

# Select collection
collection = db["transactions"]

# Load CSV into pandas
df = pd.read_csv("transactions.csv")

# Convert DataFrame to list of dictionaries
records = df.to_dict(orient="records")

# Insert into MongoDB
collection.insert_many(records)

# Verify insertion
print("Total documents:", collection.count_documents({}))

client.close()
