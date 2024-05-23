from elasticsearch import Elasticsearch
import time

class ElasticsearchClient:
    def __init__(self, host="localhost", port="9200"):
        self.host = host
        self.port = port
        self.client = Elasticsearch(["http://"+self.host+":"+self.port])

        while not self.ping():
           print("Waiting for Elasticsearch to start...")
           time.sleep(1)
        if not self.client.indices.exists(index="search_index"):
            self.create_index("search_index")
            # Define the mapping for the index
            mapping = {
                'properties': {
                    "id": {'type': 'integer'},
                    "name": {
                        'type': 'text',
                        'fields': {
                            'keyword': {'type': 'keyword'}
                        }
                    },
                    "type": {
                        'type': 'text',
                        'fields': {
                            'keyword': {'type': 'keyword'}
                        }
                    },
                    "description": {
                        'type': 'text',
                        'fields': {
                            'keyword': {'type': 'keyword'}
                        }
                    }
                }
            }
            self.create_mapping('search_index',mapping)
        
        

    def ping(self):
        return self.client.ping()
    
    def clearIndex(self, index_name):
        if self.client.indices.exists(index=index_name):
            # If the index exists, delete it
            return self.client.indices.delete(index=index_name)
        else:
            # If the index does not exist, do nothing
            return None
        return
    
    def close(self):
        self.client.close()
        return

    def create_index(self, index_name):
        return self.client.indices.create(index=index_name)
    
    def create_mapping(self, index_name, mapping):
        return self.client.indices.put_mapping(index=index_name, body=mapping)
    
    def search(self, index_name, query):
        return self.client.search(index=index_name, body=query)
    
    def index_document(self, index_name, document):
        return self.client.index(index=index_name, document=document)
    

    
    
    