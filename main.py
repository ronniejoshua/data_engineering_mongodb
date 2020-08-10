from pymongo import MongoClient
from mongodb_base import connection_string

if __name__ == "__main__":

    # Client connects to MongoDB Server
    client = MongoClient(connection_string)
