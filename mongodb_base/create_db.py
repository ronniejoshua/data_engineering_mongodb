import requests


def create_db_collections(mongo_client):
    # client is a dictionary of databases
    db = mongo_client["nobel"]

    for collection_name in ['prizes', 'laureates']:
        # collect the data from the API
        response = requests.get("http://api.nobelprize.org/v1/{}.json".format(collection_name[:-1]))

        # convert the data to json
        documents = response.json()[collection_name]

        # create collections on the fly
        # database is a dictionary of collections
        db[collection_name].insert_many(documents)


def get_database_names(mongo_client):
    # save a list of names of the databases managed by client
    db_names = mongo_client.list_database_names()
    print(db_names)


def get_database_collections(mongo_client, db_name):
    # save a list of names of the collections managed by the 'nobel' database
    nobel_coll_names = mongo_client[db_name].list_collection_names()
    print(nobel_coll_names)


def check_doc_structure(mongo_client):
    # Connect to the "nobel" database
    db = mongo_client["nobel"]

    # Retrieve sample prize and laureate documents
    prize = db.prizes.find_one()
    laureate = db.laureates.find_one()

    # Print the sample prize and laureate documents
    print(prize)
    print(laureate)
    print(type(laureate))


def get_document_fields(mongo_client):
    # Connect to the "nobel" database
    db = mongo_client["nobel"]

    # Retrieve sample prize and laureate documents
    prize = db.prizes.find_one()
    laureate = db.laureates.find_one()

    # Get the list of fields present in each type of document
    prize_fields = list(prize.keys())
    laureate_fields = list(laureate.keys())

    print(prize_fields)
    print(laureate_fields)
