#! /usr/bin/python2.6
# coding: utf-8
import gdata
import gdata.spreadsheet
from gdata.spreadsheet.text_db import DatabaseClient

_api = None
def setdefaultapi(api):
    global _api
    _api = api

class API(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password
    def auth(self):
        try:
            self.client = DatabaseClient(self.username, self.password)
        except Exception, e:
            raise e
        self.tblcache = {}
        return self

    def getTable(self, cls):
        name = cls.__name__
        key = cls.spreadsheet_key

        if name not in self.tblcache:
            kwargs = {}
            if key is None:
                kwargs['name'] = name
            else:
                kwargs['spreadsheet_key'] = key
            db = self.client.GetDatabases(**kwargs)[0]
            tbl = db.GetTables()[0]
            self.tblcache[name] = tbl
        return self.tblcache[name]

    def sync(self, cls):
        name = cls.__name__
        if not cls.spreadsheet_key is None:
            db = self.client.GetDatabases(spreadsheet_key = cls.spreadsheet_key)[0]
            self.tblcache[name] = db.GetTables()[0]
            return
        db = self.client.CreateDatabase(name = name)
        tbl = db.CreateTable(name, [colname for colname,_ in cls.cols])
        db.GetTables()[0].Delete()
        self.tblcache[name] = tbl

    def update(self, cls, obj):
        tbl = self.getTable(cls)
        if obj.id is None:
            record = tbl.AddRecord(self.obj2dict(obj))
        else:
            record = tbl.GetRecord(row_id = obj.id)
            record.Push()
        return self.record2obj(record, obj)
    def obj2dict(self, obj):
        #print obj.__dict__
        return dict((k, unicode(v)) for k,v in obj.__dict__.items())
    def record2obj(self, record, obj):
        # clsの型ベース
        obj.row_id = record.row_id
        for colname, coltype in obj.__class__.cols:
            value = None
            if coltype == 'timestamp': # TODO:
                value = record.content[colname]
            else:
                value = coltype(record.content[colname])
            setattr(obj, colname, value)
        return obj
    def delete(self, obj):
        obj.Delete()
    def find(self, cls, q):
        lst = self.getTable(cls).FindRecords(q)
        fn = self.record2obj
        return map(lambda r: fn(r, cls()), lst)
    def initbyid(self, entry, row_id):
        record = self.GetRecord(row_id = row_id)
        for k,v in record.content.items():
            entry.__dict__[k] = v
            setattr(entry, key, record.content[key])
        entry.id = row_id
    def reader(self, cls, wherecond = None):
        tbl = self.getTable(cls)
        for entry in self.getTable(cls).entry:
            yield entry
            #obj = cls()
            #yield self.record2obj(entry, cls)

class Model(object):
    cols = [] # (name, type)
    spreadsheet_key = None
    def __init__(self, *args, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)
        if 'id' not in kwargs:
            self.__dict__['id'] = None
        else:
            self.initbyid(kwargs['id'])
    def __getattr__(self, name):
        return self.__dict__[name]
    def __setattr__(self, name, value):
        self.__dict__[name] = value
    def initbyid(self, row_id):
        _api.initbyid(self, row_id)
    def update(self):
        _api.update(self.__class__, self)
    def delete(self):
        _api.delete(self)
    @classmethod
    def find(cls, q):
        return _api.find(cls, q)
    @classmethod
    def sync(cls):
        _api.sync(cls)
    @classmethod
    def reader(cls):
        return _api.reader(cls)


