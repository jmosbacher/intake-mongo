
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

from intake.source import base
from . import __version__


def flatten_nested_dict(d):
    flat = {}
    for k0, v0 in d.items():
        if isinstance(v0, dict):
            for k1, v1 in flatten_nested_dict(v0).items():
                flat[(k0,) + k1] = v1
        else:
            flat[(k0,)] = v0
    return flat
    
def dicts_to_table(ds):
    N = len(ds)
    table = defaultdict(lambda:[float("nan")]*N)
    flat_ds = [flatten_nested_dict(d) for d in ds]
    for i,d in enumerate(flat_ds):
        for k,v in d.items():
            table[k][i] = v
    return table
    
class MongoDictSource(base.DataSource):
    name = 'mongo'
    container = 'python'
    partition_access = True
    version = __version__

    def __init__(self, uri, db, collection, connect_kwargs=None,
                 find_kwargs=None, _id=False, metadata=None, chunksize=100):
        """Load data from MongoDB
        Parameters
        ----------
        uri: str
            a valid mongodb uri in the form
            '[mongodb:]//host:port'.
            The URI may include authentication information, see
            http://api.mongodb.com/python/current/examples/authentication.html
        db: str
            The database to access
        collection: str
            The collection in the database that will act as source;
        connect_kwargs: dict or None
            Parameters passed to the pymongo ``MongoClient``, see
            http://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
            This may include security information such as passwords and
            certificates
        find_kwargs: dict or None
            Parameters passed to the pymongo ``.find()`` method, see
            http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
            This includes filters, choice of fields, sorting, etc.
        _id: bool (False)
            If False, remove default "_id" field from output
        metadata: dict
            The metadata to keep
        chunksize: int
            The number of documents to return for each partition
        """
        super().__init__(metadata=metadata)
        self._uri = uri
        self._db = db
        self._collection = collection
        self._connect_kwargs = connect_kwargs or {}
        self._id = _id
        self._chunksize = chunksize
        kw = find_kwargs or {}
        if self._id is False:
            # https://stackoverflow.com/a/12345646/3821154
            if 'projection' in kw:
                pro = kw.pop('projection')
                if isinstance(pro, (list, tuple)):
                    pro = {k: True for k in pro}
                pro['_id'] = False
            else:
                pro = {'_id': False}
            kw['projection'] = pro
            kw["filter"] = kw.get("filter", {})
        self._find_kwargs = kw
        self.collection = None

    def post_process(self, data):
        return list(data)

    def _get_schema(self):
        if self.collection is None:
            import pymongo
            self.client = pymongo.MongoClient(self._uri, **self._connect_kwargs)
            self.collection = self.client[self._db][self._collection]
        ndocs = self.collection.count_documents({})
        if ndocs<self._chunksize:
            self._chunksize = ndocs
        part0 = self.read_partition(0)[0]
        ncols = len(part0.keys())
        npart = int(math.ceil(ndocs/self._chunksize))
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=(ndocs,ncols),
                           npartitions=npart,
                           extra_metadata={})

    def _get_partition(self, i):
        self._load_metadata()
        start = i*self._chunksize
        data = self.post_process(self.collection.find(**self._find_kwargs)[start:start+self._chunksize])
        return data

    def read(self):
        self._load_metadata()
        data = self.post_process(self.collection.find(**self._find_kwargs))
        return data

    def _close(self):
        if self.client:
            self.client.close()
            self.client = None
        self.collection = None

class MongoDataFrameSource(MongoDictSource):
    name = 'mongodf'
    container = 'dataframe'
    partition_access = True
    version = __version__

    def post_process(self, data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Must have pandas installed to use {} plugin".format(self.name))
        data = super().post_process(data)
        table = dicts_to_table(data)
        return pd.DataFrame(table)

    def to_dask(self):
        try:
            import dask.dataframe as dd
        except ImportError:
            raise ImportError("Must have dask installed to use this method")
        df = self.read()
        return dd.from_pandas(df, chunksize=self._chunksize)
