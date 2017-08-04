from pymongo import MongoClient, errors
import pprint

class DB:
    def __init__(self, connection_string, database):
        self._client = MongoClient(connection_string)
        self._db = self._client[database]


    def insert_one(self, collection_name, document):
        collection = self._db[collection_name]

        try:
            result = collection.insert_one(document)
            print(result)
        except errors.DuplicateKeyError as e:
            print(e.details)

    def bulk_insert(self, collection_name, documents:list):
        collection = self._db[collection_name]
        bulk = collection.initialize_unordered_bulk_op()

        for doc in documents:
            bulk.insert(doc)

        try:
            result = bulk.execute()
            pprint.pprint(result)
        except errors.BulkWriteError as e:
            pprint.pprint(e.details)


    def find(self, collection_name, **kwargs):
        collection = self._db[collection_name]
        return collection.find(**kwargs)

    def get_collection(self, collection_name):
        return self._db[collection_name]

