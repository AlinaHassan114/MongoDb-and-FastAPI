import pandas as pd
from pymongo import MongoClient, UpdateOne
import os
import sys

def create_indexes(db):
    """Create required indexes"""
    print("Creating indexes...")
    
    # Books indexes
    db.books.create_index([("title", 1), ("authors", 1)])
    db.books.create_index([("average_rating", -1)])
    db.books.create_index([("book_id", 1)], unique=True)
    
    # Ratings indexes
    db.ratings.create_index([("book_id", 1)])
    db.ratings.create_index([("user_id", 1), ("book_id", 1)], unique=True)
    
    # Tags indexes
    db.tags.create_index([("tag_id", 1)], unique=True)
    db.tags.create_index([("tag_name", 1)])
    
    # Book tags indexes
    db.book_tags.create_index([("tag_id", 1)])
    db.book_tags.create_index([("goodreads_book_id", 1)])
    
    # To read indexes
    db.to_read.create_index([("user_id", 1), ("book_id", 1)], unique=True)
    
    print("Indexes created successfully")

def load_collection(db, collection_name, url, dtype=None):
    """Load CSV data into MongoDB collection"""
    print(f"Loading {collection_name}...")
    
    try:
        df = pd.read_csv(url, dtype=dtype)
        
        # Convert NaN to None for MongoDB compatibility
        df = df.where(pd.notnull(df), None)
        
        records = df.to_dict('records')
        
        # Clear existing data
        db[collection_name].delete_many({})
        
        # Insert new data
        if records:
            result = db[collection_name].insert_many(records)
            print(f"  Inserted {len(result.inserted_ids)} records")
        
    except Exception as e:
        print(f"Error loading {collection_name}: {str(e)}")
        return False
    
    return True

def main():
    """Main ingestion function"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "goodbooks")
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    print("Starting data ingestion...")
    
    # data types for each collection
    dtypes = {
        "books": {
            'book_id': 'Int64',
            'goodreads_book_id': 'Int64', 
            'original_publication_year': 'Int64',
            'ratings_count': 'Int64'
        },
        "ratings": {
            'user_id': 'Int64',
            'book_id': 'Int64',
            'rating': 'Int64'
        },
        "tags": {
            'tag_id': 'Int64'
        },
        "book_tags": {
            'goodreads_book_id': 'Int64',
            'tag_id': 'Int64',
            'count': 'Int64'
        },
        "to_read": {
            'user_id': 'Int64',
            'book_id': 'Int64'
        }
    }
    
    base_url = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/"
    
    collections = [
        ("books", f"{base_url}books.csv"),
        ("ratings", f"{base_url}ratings.csv"), 
        ("tags", f"{base_url}tags.csv"),
        ("book_tags", f"{base_url}book_tags.csv"),
        ("to_read", f"{base_url}to_read.csv")
    ]
    
    success_count = 0
    for collection_name, url in collections:
        if load_collection(db, collection_name, url, dtypes.get(collection_name)):
            success_count += 1
    
    if success_count == len(collections):
        create_indexes(db)
        print("Data ingestion completed successfully!")
    else:
        print(f"Data ingestion completed with errors. {success_count}/{len(collections)} collections loaded.")
        sys.exit(1)

if __name__ == "__main__":
    main()