from couchdbkit import client, CouchdbResource
from couchdbkit.exceptions import ResourceConflict
import uuid
import time
import hashlib
import os
import random
from threading import Thread

import unittest
from nose import tools
from testconfig import config
from couchdbkit import client
import logger


def prepareUserDoc(user_doc, new_password=None):
    user_prefix = "org.couchdb.user:"
    if not "_id" in user_doc:
        user_doc["_id"] = user_prefix + user_doc["name"]
    if new_password:
        if isinstance(new_password, unicode):
            password_8bit = new_password.encode('UTF-8')
        else:
            password_8bit = new_password
        salt = hashlib.sha1()
        salt.update(os.urandom(60))
        user_doc["salt"] = salt.hexdigest()
        hash = hashlib.sha1()
        hash.update(password_8bit + user_doc["salt"])
        user_doc["password_sha"] = hash.hexdigest()
    user_doc["type"] = "user"
    if not "roles" in user_doc:
        user_doc["roles"] = []

    return user_doc
    
def modify_server(settings):
    resource = CouchdbResource()
    for s in settings:
        confpath = "/_config/{0}/{1}".format(s["section"], s["key"])
        payload = {}
        payload[s["key"]] = s["value"]
        info = resource.put(path=confpath, payload=payload, headers={"X-Couch-Persist": "false"})

def get_userdb():
    resource = CouchdbResource()
    path = "/_config/couch_httpd_auth/authentication_db"
    info = resource.get(path)
    
    return info.json_body

def session():
    resource = CouchdbResource()
    info = resource.get("/_session")
    return info.json_body

def login(name, password):
    resource = CouchdbResource()
    info = resource.request("POST", path="/_session",
            headers = {"Content-Type": "application/json",
                       "X-CouchDB-WWW-Authenticate": "Cookie"},
            payload={"name":name, "password":password})

    return info.json_body

def logout():
    resource = CouchdbResource()
    info = resource.request("DELETE", "/_session", 
                            headers = {"Content-Type": "application/json",
                                       "X-CouchDB-WWW-Authenticate": "Cookie"})
    return info.json_body

def _get_db_name():
    name = "doctests-{0}".format(str(uuid.uuid4())[:6])
    return name

def generateSecret(length):
    tab = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    #secret = '3aa9dd562cbc1582ef4f4e1f356dd075';
    secret = ""
    for i in range(length):
      secret += tab[random.randint(0,63)]
 
    return secret

class UserTests(unittest.TestCase):

    def setUp(self):
        self.log = logger.logger("usertests")
        
        url = "http://127.0.0.1:5984/"
        self.server = client.Server(url, full_commit=False)
        self.num_user = 100
        self.user_dbname = get_userdb()
        self.user_db = self.server.get_or_create_db(self.user_dbname)

    def tearDown(self):
        all_dbs = self.server.all_dbs()
        for db in self.server.all_dbs():
            if db.find("doctest") != -1:
                self.server.delete_db(db)

    def _upload_docs(self, db, docs):
        db.bulk_save(docs)

    def _create_user_docs(self, num_user, baseid):
        docs = []
        for i in range(num_user):
            name = "user_{0}".format(i+baseid)
            passwd = "password_{0}_{1}".format(i+baseid, name)
            doc = prepareUserDoc({"name": name, "roles": ["dev"]}, passwd)
            if not self.user_db.doc_exist(doc["_id"]) :
                docs.append(doc)
        
        return docs

    def createUsers(self, db, num_user, num_writer):
        for i in range(num_writer):
            docs = self._create_user_docs(num_user, i*num_writer)
            writer = Thread(target=self._upload_docs, args=(db, docs,))
            writer.start()
            writer.join()

    def actor(self, server, user, password):
        db_name = _get_db_name()
        db = server.get_or_create_db(db_name)
        self.log.info("user:"+user+" pwd:"+password)
        try:
            tools.ok_(login(user, password)['ok'])
        except Exception:
            self.log.info("Exception launched: Name or password is incorrect")
            pass

        num_doc = 100
        id_range = 1000
        for i in range(num_doc):
            doc = {"_id":"id_{0}".format(i), "a":random.randint(0, id_range), "b":1}
            res = db.save_doc(doc)
            fetched = db.get(res['id'])
            doc["a"] = random.randint(0,id_range)
            res = db.save_doc(doc)
        try:
            tools.ok_(logout()['ok'])
        except Exception:
            pass

    def _test_multiple_users_multi_db(self):
        num_user = 1000
        num_writer = 20
        self.createUsers(self.user_db, num_user, num_writer)

        for i in range(num_user*num_writer):
            name = "user_{0}".format(i)
            passwd = "password_{0}_{1}".format(i, name)
            user = Thread(target=self.actor, args=(self.server, name, passwd,))
            user.start()
            user.join()

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

    def _crud_db(self, db, num_docs):
        num_del = random.randint(0, num_docs) / 10
        del_ids = []
        for i in range(num_del):
            del_ids.append(random.randint(0, num_docs))

        for doc in range(num_docs):
            id = "crud_{0}".format(random.randint(0, num_docs))
            try:
                fetched = db.get(id)
                fetched["c"] = "new field"
                db.save_doc(fetched)
            except Exception:
                self.log.info(id)
                pass
        for i in range(num_del):
            id = "crud_{0}".format(del_ids[i])
            try:
                db.del_doc(id)
            except Exception:
                pass

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

    def heavy_actor(self, db, user, password, total_doc):
        self.log.info("user:"+user+" pwd:"+password)
        try:
            tools.ok_(login(user, password)['ok'])
        except Exception:
            self.log.info("Exception launched: Name or password is incorrect")
            pass

        running = Thread(target=self._crud_db, args=(db,total_doc,))
        running.start()   
        running.join()

        try:
            tools.ok_(logout()['ok'])
        except Exception:
            pass
            
    def test_multiple_users_single_db(self):
        num_user = 1
        num_writer = 2
        self.createUsers(self.user_db, num_user, num_writer)

        db_name = _get_db_name()
        work_db = self.server.get_or_create_db(db_name)

        num_doc = 10
        num_writer = 2

        self._quick_upload_datdabase(work_db, num_doc, num_writer)
        all_docs = work_db.all_docs()
        self.assertEqual(all_docs.total_rows, num_writer * num_doc)
        
        self._multi_design_view(work_db)

        for i in range(self.num_user):
            name = "user_{0}".format(i)
            passwd = "password_{0}_{1}".format(i, name)
            user = Thread(target=self.heavy_actor, args=(work_db, name, passwd, num_writer * num_doc))
            user.start()
            user.join()

        rows = work_db.view("test/all_docs")
        rows = work_db.view("test/multi_emit", startkey=100, endkey=300)
        rows = work_db.view("test/summate", reduce=True, startkey_docid="1000", endkey_docid="4000")
        rows = work_db.view("test/get_by_a", descending=True)
        rows = work_db.view("test/get_by_b", group=True)
        rows = work_db.view("test/get_by_c", group=True)
        rows = work_db.view("test/get_by_ab", group_level=1)
        rows = work_db.view("test/get_even")
        rows = work_db.view("test/get_odd")