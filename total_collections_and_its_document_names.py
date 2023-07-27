from pymongo import MongoClient

def print_nested_keys(document, parent_key=""):
    if isinstance(document, dict):
        for key, value in document.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            print(new_key)
            print_nested_keys(value, new_key)

def get_first_documents():
    client = MongoClient('mongodb://localhost:27017')  # Replace with your MongoDB connection string
    database = client.mydb  # Replace with your database name

    collection_names = database.list_collection_names()
    first_documents = {}

    for collection_name in collection_names:
        collection = database[collection_name]
        first_document = collection.find_one()
        first_documents[collection_name] = first_document
 
    return first_documents

first_docs = get_first_documents()
for collection_name, document in first_docs.items():
    print(f"Collection: {collection_name}")
    print("Nested Keys:")
    print_nested_keys(document)
    print()

