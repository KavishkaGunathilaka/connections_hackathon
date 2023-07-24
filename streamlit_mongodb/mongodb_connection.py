from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import pandas as pd
import pymongo
from pymongo.server_api import ServerApi

class MongoDBConnection(ExperimentalBaseConnection[pymongo.MongoClient]):
    def _connect(self, **kwargs) -> pymongo.MongoClient:
        password = self._secrets['password']
        username = self._secrets['username']
        return pymongo.MongoClient("mongodb+srv://%s:%s@cluster0.cqtg6tk.mongodb.net/?retryWrites=true&w=majority" % (username, password), server_api=ServerApi('1'), **kwargs)
    
    def query(self, query: dict, db: str, col: str,  limit: int = 0, ttl: int = 3600, **kwargs) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _query(query: dict, **kwargs) -> pd.DataFrame:
            myclient = self._instance
            mydb = myclient[db]
            mycol = mydb[col]
            mydoc = mycol.find(query, **kwargs).limit(limit)
            list_cur = list(mydoc)
            return pd.DataFrame(list_cur)
        return _query(query, **kwargs)