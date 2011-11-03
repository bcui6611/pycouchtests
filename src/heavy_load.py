import random
import time
from threading import Thread
import unittest
from nose import tools
import uuid
from testconfig import config
from couchdbkit import client
import logger

class HeavyLoadTests(unittest.TestCase):

    nodes = []
    servers = []
    cleanup_dbs = []

    def setUp(self):
        self.log = logger.logger("basictests")
        
        node_names = ['couchdb-local', 'couchdb-remote-1', 'couchdb-remote-2']
        for name in node_names:
            node = config[name]
            self.nodes.append(node)
            url = "http://{0}:{1}/".format(node['ip'], node['port'])
            server = client.Server(url, full_commit=False)
            self.servers.append(server)
        
    def tearDown(self):
        for db in self.cleanup_dbs:
            for server in self.servers:
                try:
                    #pass
                    server.delete_db(db)
                except Exception:
                    pass

        all_dbs = self.servers[0].all_dbs()
        for db in all_dbs:
            if db.find("doctest") != -1:
                self.servers[0].delete_db(db)
        self.cleanup_dbs = []

    def _get_db_name(self):
        name = "doctests-{0}".format(str(uuid.uuid4())[:6])
        self.cleanup_dbs.append(name)
        return name

    def _isodd(self, num):
        return num & 1 and True or False

    def _random_docs(self, howmany=1, baseid=0):
        docs = []
        for i in range(howmany):
            id = "crud_{0}".format(i+baseid)
            v1 = random.randint(0, 100)
            v2 = random.randint(0, 10000)
            if self._isodd(i):
                type = "odd" 
            else:
                type = "even"
            #have random key-values here ?
            doc = {"_id": id, "a": v1, "b": v2, "c": str(uuid.uuid4())[:6], "type":type }
            docs.append(doc)
        return docs

    def _populate_database(self, server, server_ip, num_db, num_doc, num_attachment, first_only=False):
        text_attachment = "a text attachment"

        local_dbs = []
        for i in range(num_db):
            db_name = self._get_db_name()
            local_dbs.append(db_name)
            db = server.get_or_create_db(db_name)
            if i == 0 or not first_only:
                for j in range(num_doc):
                    if self._isodd(j):
                        doc = {"_id": str(j), "a": j, "b": str(uuid.uuid4())[:6], "node": server_ip, "type": "odd" }
                    else:
                        doc = {"_id": str(j), "a": j, "b": str(uuid.uuid4())[:6], "node": server_ip, "type": "even" }
                    db.save_doc(doc)
                    for k in range(num_attachment):
                        db.put_attachment(doc, text_attachment, "test_" + str(k), "text/plain")

        return local_dbs

    def _upload_docs(self, db, docs):
        db.bulk_save(docs)

    def _quick_upload_datdabase(self, db, num_doc, num_writer):
        for i in range(num_writer):
            docs = self._random_docs(num_doc, i*num_doc)
            writer = Thread(target=self._upload_docs, args=(db, docs,))
            writer.start()
            writer.join()

    def _multi_design_view(self, db):
        design_name = "_design/test"
        design_doc = {
            "_id": design_name,
            "language": "javascript",
            "views": {
                "all_docs": {
                    "map": "function(doc) { emit([doc.a, doc.b, doc.type], 1) };",
                    "reduce": "function(keys, values) { return _count; }"
                },
                "multi_emit": {
                    "map": "function(doc) {for(var i = 0 ; i < 3 ; i++) { emit(i, doc.a) ; } }"
                },
                "summate": {
                    "map": "function (doc) {emit(doc.type, 1)};",
                    "reduce": "function (keys, values) { return _count; };"
                },
                "get_by_a" : {
                    "map": "function(doc) { if (doc.a > 50) emit(doc.a, 1) };"
                },
                "get_by_b" : {
                    "map": "function(doc) { if (doc.b < 1000) emit(doc.b, 1) };",
                    "reduce": "function(keys, values) { return _sum; };"
                },
                "get_by_c" : {
                    "map": "function(doc) { emit(doc.c, [doc.a, doc.type]); };"
                },
                "get_by_ab" : {
                    "map": "function(doc) { if (a > b) emit([doc.a, doc.b], 1); }",
                    "reduce": "function(keys, values) { return _count; }"
                },
                "get_even" : {
                    "map": """function(doc) { if (doc.type == "even") emit(null, 1);}"""
                },
                "get_odd" : {
                    "map": """function(doc) { if (doc.type == "odd") emit(null, 1);}"""
                }
            }
        };

        if not db.doc_exist(design_name):
            db.save_doc(design_doc)

    def test_heavy_load_single_db(self):
        num_db = 1
        num_doc = 10000
        num_writer = 20
        db_name = self._get_db_name()
        db = self.servers[0].get_or_create_db(db_name)
        self._quick_upload_datdabase(db, num_doc, num_writer)
        all_docs = db.all_docs()
        self.assertEqual(all_docs.total_rows, num_writer * num_doc)
        
        self._multi_design_view(db)
        rows = db.view("test/all_docs")
        rows = db.view("test/multi_emit", startkey=100, endkey=300)
        rows = db.view("test/summate", reduce=True, startkey_docid="1000", endkey_docid="4000")
        rows = db.view("test/get_by_a", descending=True)
        rows = db.view("test/get_by_b", group=True)
        rows = db.view("test/get_by_c", group=True)
        rows = db.view("test/get_by_ab", group_level=1)
        rows = db.view("test/get_even")
        rows = db.view("test/get_odd")
        




    

