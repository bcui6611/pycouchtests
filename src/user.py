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

    def createUsers(self):
        for i in range(self.num_user):
            name = "user_{0}".format(i)
            passwd = "password_{0}_{1}".format(i, name)
            doc = prepareUserDoc({"name": name, "roles": ["dev"]}, passwd)
            if not self.user_db.doc_exist(doc["_id"]) :
                self.user_db.save_doc(doc)

    def actor(self, server, user, password):
        db_name = _get_db_name()
        db = server.get_or_create_db(db_name)

        login(user, password)

        for i in range(100):
            doc = {"_id":"id_{0}".format(i), "a":random.randint(0, 1000), "b":1}
            res = db.save_doc(doc)
            fetched = db.get(res['id'])
            doc["a"] = random.randint(0,1000)
            res = db.save_doc(doc)

        logout()

    def test_multiple_users(self):
        self.createUsers()

        for i in range(self.num_user):
            name = "user_{0}".format(i)
            passwd = "password_{0}_{1}".format(i, name)
            user = Thread(target=self.actor, args=(self.server, name, passwd,))
            user.start()
            user.join()



