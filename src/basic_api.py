import random
import unittest
from nose import tools
import uuid
from testconfig import config
from couchdbkit import client
import logger

class BasicTests(unittest.TestCase):

    cleanup_dbs = []

    def setUp(self):
        node = config['couchdb-local']
        self.log = logger.logger("basictests")
        url = "http://{0}:{1}/".format(node['ip'], node['port'])
        self.server = client.Server(url, full_commit=False)
        self.node = node

    def tearDown(self):
        for db in self.cleanup_dbs:
            try:
                #self.server.delete_db(db)
                pass
            except Exception:
                pass

        #all_dbs = [db for db in self.server.all_dbs()]
        #for db in all_dbs:
        #    if db.find("doctest") != -1:
        #        self.server.delete_db(db)
        self.cleanup_dbs = []

    def _get_db_name(self):
        name = "doctests-{0}".format(str(uuid.uuid4())[:6])
        self.cleanup_dbs.append(name)
        return name

    def test_create(self):
        db_name = self._get_db_name()
        ##self.server.create_db(db_name)
        db = self.server.get_or_create_db(db_name)
        self.assertTrue( db.info()['db_name'] == db_name)

    def test_doccount(self):
        db_name = self._get_db_name()
        self.assertEqual(self.server.get_or_create_db(db_name).info()['doc_count'], 0, "doc_count is not equal to zero.")

    def test_docsave(self):
        db_name = self._get_db_name()
        doc = {"_id":"0","a":1,"b":1}
        res = self.server.get_or_create_db(db_name).save_doc(doc)
        tools.eq_(res['id'], '0')
        tools.ok_(res['rev'])

    def test_docdelete(self):
        db_name = self._get_db_name()
        doc = {"_id":"0","a":1,"b":1}
        db  = self.server.get_or_create_db(db_name)
        id = db.save_doc(doc)['id']
        fetched = db.get(id)
        db.delete_doc(fetched)
        try:
            db.get(id)
        except:
            pass

    def test_multiplecreate(self):
        db_names = []
        for i in range(2):
            db_names.append(self._get_db_name())
        for db in db_names:
            self.server.create_db(db)
        #verify all dbs
        all_dbs = self.server.all_dbs()
        for db_name in db_names:
            exist = False
            for couchdb_db in all_dbs:
                if couchdb_db == db_name:
                    exist = True
                    break
            tools.ok_(exist, "db {0} was created but not listed in couchdb.all_dbs".format(db_name))

    def _random_doc(self, howmany=1):
        docs = []
        for i in range(howmany):
            id = "{0}".format(i)
            k1 = "a"
            v1 = random.randint(0, 10000)
            k2 = "b"
            v2 = random.randint(0, 10000)
            #have random key-values here ?
            doc = {"_id": id, k1: v1, k2: v2}
            docs.append(doc)
        return docs

    def test_multipledocs(self):
        db_name = self._get_db_name()
        db = self.server.get_or_create_db(db_name)
        docs = self._random_doc(10)
        for doc in docs:
            db.save_doc(doc)
        tools.ok_(self.server.get_or_create_db(db_name).info()['doc_count'] == len(docs))

    def _query(self, db, view_name, map, reduce=None):
        design_name = "_design/test";
        design_doc = {
                "_id": design_name,
                "language": "javascript",
                "views": {
                     view_name : {
                     "map": map
                     }
                }
            }
        if reduce:
            design_doc = {
                "_id": design_name,
                "language": "javascript",
                "views": {
                     view_name : {
                     "map": map,
                     "reduce": reduce
                     }
                }
            }
        if not db.doc_exist(design_name):
            db.save_doc(design_doc)

        return db.view('test/' + view_name)

    def test_maponekey(self):
        query = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        db = self.server.get_or_create_db(db_name)
        db.save_doc(doc1)
        db.save_doc(doc2)
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 1)
        tools.eq_(results.first()['value'], doc1["b"])

    def test_mapqueryafterupdate(self):
        query = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        db = self.server.get_or_create_db(db_name)
        db.save_doc(doc1)
        db.save_doc(doc2)
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 1)
        fetched = db.get("1")
        fetched["a"] = 4
        db.save_doc(fetched)
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 2)
        doc3 = {"_id": "2", "a": 4, "b": 4}
        doc4 = {"_id": "3", "a": 5, "b": 5}
        doc5 = {"_id": "4", "a": 6, "b": 6}
        doc6 = {"_id": "5", "a": 7, "b": 7}
        db.bulk_save([doc3,doc4,doc5,doc6])
        results = results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 3)

    def test_mapqueryafterdelete(self):
        query = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        db = self.server.get_or_create_db(db_name)
        db.save_doc(doc1)
        db.save_doc(doc2)
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 1)
        fetched = db.get("1")
        fetched["a"] = 4
        db.save_doc(fetched)
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 2)
        doc3 = {"_id": "2", "a": 4, "b": 4}
        doc4 = {"_id": "3", "a": 5, "b": 5}
        doc5 = {"_id": "4", "a": 6, "b": 6}
        doc6 = {"_id": "5", "a": 7, "b": 7}
        db.bulk_save([doc3,doc4,doc5,doc6])
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 3)
        #now delete doc3
        fetched = db.get(doc3["_id"])
        db.delete_doc(fetched)
        results = self._query(db, "maponekey", query)
        tools.eq_(results.total_rows, 2)

    def test_reduceonekey(self):
        map = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        reduce = """function(keys, values) {
            return sum(values);
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        doc3 = {"_id": "2", "a": 4, "b": 40}
        db = self.server.get_or_create_db(db_name)
        db.save_doc(doc1)
        db.save_doc(doc2)
        db.save_doc(doc3)
        results = self._query(db, "maponekey", map, reduce)
        tools.eq_(results.total_rows, 1)
        tools.eq_(results.first()['value'], doc1["b"] + doc3["b"])

    def _doc_equals(self, doc1, doc2):
        #two way equal ?
        ok = True
        for k in doc1:
            if k != "rev":
                ok = k in doc2 and doc1[k] == doc2[k]
            if not ok:
                break
        if ok:
            for k in doc2:
                if k != "rev":
                    ok = k in doc1 and doc1[k] == doc2[k]
                if not ok:
                    break
        return ok

    def test_get(self):
        db_name = self._get_db_name()
        db = self.server.get_or_create_db(db_name)
        doc = {"_id":"0","a":1,"b":1}
        res = db.save_doc(doc)
        fetched = db.get(res['id'])
        tools.ok_(self._doc_equals(doc,fetched))

    def test_update(self):
        db_name = self._get_db_name()
        db = self.server.get_or_create_db(db_name)
        doc = {"_id":"0","a":1,"b":1}
        res = db.save_doc(doc)
        fetched = db.get(res['id'])
        tools.ok_(self._doc_equals(doc,fetched))
        doc["a"] = 10000
        res = db.save_doc(doc)
        tools.eq_(res['id'],"0")

        fetched = self.server[db_name].get(res['id'])
        tools.ok_(self._doc_equals(doc,fetched))