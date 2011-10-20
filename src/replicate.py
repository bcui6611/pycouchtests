import random
from threading import Thread
import unittest
from nose import tools
import uuid
from testconfig import config
from couchdbkit import client
import logger

class BasicTests(unittest.TestCase):

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

        #all_dbs = [db for db in self.server.all_dbs()]
        #for db in all_dbs:
        #    if db.find("doctest") != -1:
        #        self.server.delete_db(db)
        self.cleanup_dbs = []

    def _get_db_name(self):
        name = "doctests-{0}".format(str(uuid.uuid4())[:6])
        self.cleanup_dbs.append(name)
        return name

    def _isodd(self, num):
        return num & 1 and True or False

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

    def test_load_db(self):
        num_db = 10
        num_doc = 100
        num_attachment = 0
        self._popoluate_database(num_db, num_doc, num_attachment)

    def test_load_db_with_attachment(self):
        num_db = 10
        num_doc = 100
        num_attachment = 5
        self._popoluate_database(num_db, num_doc, num_attachment)
        
    def test_local_to_local(self):
        num_doc=5
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 2, num_doc, 5, True)
        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        source_url = url + local_dbs[0]
        target_url = url + local_dbs[1]
        self.servers[0].replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True)
        if not continuous: 
            db = self.servers[0].get_or_create_db(local_dbs[1])
            result = db.all_docs()
            self.assertEqual(result.total_rows, num_doc)

    def _replicate_db(self, source_server, source_node, remote_node, local_db, continuous):
        source_url = "http://{0}:{1}".format(source_node['ip'], source_node['port']) + "/" + local_db
        target_url = "http://{0}:{1}".format(remote_node['ip'], remote_node['port']) + "/" + local_db
        source_server.replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True)

    def test_local_to_remote(self):
        num_doc=5
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 5, num_doc, 5, False)
        for db in local_dbs:
            for remote_node in self.nodes[1:]:
                replica = Thread(target=self._replicate_db, args=(self.servers[0], self.nodes[0], remote_node, db, continuous,))
                replica.start()
                replica.join()

    def test_local_to_local_with_attachment(self):
        num_doc=5
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 2, num_doc, 5, True)
        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        source_url = url + local_dbs[0]
        target_url = url + local_dbs[1]
        self.servers[0].replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True)
        if not continuous: 
            db = self.servers[0].get_or_create_db(local_dbs[1])
            result = db.all_docs()
            self.assertEqual(result.total_rows, num_doc)

    def _filter(self, db_name):
        design_name = "_design/test_filter";
        design_doc = {
                "_id": design_name,
                "language": "javascript",
                "filters": {
                     "even": """function(doc, req) {
    if (doc.type && doc.type == "even") {
        return true;
    } else {
        return false;
    }
}"""
                }
            }
        db = self.servers[0].get_or_create_db(db_name)
        if not db.doc_exist(design_name):
            db.save_doc(design_doc)

    def test_local_to_local_with_filter(self):
        num_doc=10
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 2, num_doc, 5, True)
        self._filter(local_dbs[0])
        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        source_url = url + local_dbs[0]
        target_url = url + local_dbs[1]
        self.servers[0].replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True, filter="test_filter/even")
        if not continuous: 
            db = self.servers[0].get_or_create_db(local_dbs[1])
            result = db.all_docs()
            self.assertEqual(result.total_rows, num_doc/2)

    def _compact_db(self, db_name):
        db = self.servers[0].get_or_create_db(db_name)
        db.compact()

    def test_local_to_local_while_compacting(self):
        num_doc=5
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 2, num_doc, 0, True)

        compactor = Thread(target=self._compact_db, args=(local_dbs[0],))
        compactor.start()

        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        source_url = url + local_dbs[0]
        target_url = url + local_dbs[1]
        self.servers[0].replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True)
        if not continuous: 
            db = self.servers[0].get_or_create_db(local_dbs[1])
            result = db.all_docs()
            self.assertEqual(result.total_rows, num_doc)
        compactor.join()

    def _random_doc(self, howmany=1):
        docs = []
        for i in range(howmany):
            id = "crud_{0}".format(i)
            k1 = "a"
            v1 = random.randint(0, 10000)
            k2 = "b"
            v2 = random.randint(0, 10000)
            if self._isodd(i):
                type = "odd" 
            else:
                type = "even"
            #have random key-values here ?
            doc = {"_id": id, k1: v1, k2: v2, "type":type}
            docs.append(doc)
        return docs

    def _crud_db(self, db_name):
        num_doc = 10
        db = self.servers[0].get_or_create_db(db_name)
        docs = self._random_doc(num_doc)
        for doc in docs:
            db.save_doc(doc)
        
        for i in range(num_doc):
             fetched = db.get("crud_{0}".format(i))
             fetched["c"] = "new field"
             db.save_doc(fetched)
        
        #for doc in docs:
        #   db.delete_doc(doc)
        

    def test_local_to_local_while_crud(self):
        num_doc=5
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 2, num_doc, 0, True)

        running = Thread(target=self._crud_db, args=(local_dbs[0],))
        running.start()

        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        source_url = url + local_dbs[0]
        target_url = url + local_dbs[1]
        self.servers[0].replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True)
        running.join()

    def test_local_to_local_with_filter_while_crud(self):
        num_doc=10
        continuous = False
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], 2, num_doc, 5, True)
        self._filter(local_dbs[0])

        running = Thread(target=self._crud_db, args=(local_dbs[0],))
        running.start()

        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        source_url = url + local_dbs[0]
        target_url = url + local_dbs[1]
        self.servers[0].replicate(source_url, target_url, continuous=continuous, cancel=False, create_target=True, filter="test_filter/even")
        if not continuous: 
            db = self.servers[0].get_or_create_db(local_dbs[1])
            result = db.all_docs()
            self.assertEqual(result.total_rows, num_doc)

    def test_local_circle(self):
        num_doc=5
        num_db=5
        continuous = False
        url = "http://{0}:{1}".format(self.nodes[0]['ip'], self.nodes[0]['port']) + "/"
        local_dbs = local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], num_db, num_doc, 0, True)
        for i in range(num_db):
            source_db = local_dbs[i]
            if i+1 >= num_db:
                target_db =  local_dbs[0]
            else:
                target_db =  local_dbs[i+1]
            self.servers[0].replicate(url+source_db, url+target_db, continuous=continuous, cancel=False, create_target=True)
            if not continuous:
                self.assertEqual(self.servers[0].get_or_create_db(source_db).all_docs().total_rows, 
                                 self.servers[0].get_or_create_db(target_db).all_docs().total_rows)
    
    def test_local_to_remote_circle(self):
        num_db=5
        num_doc=5
        continuous = True
        local_dbs = self._populate_database(self.servers[0], self.nodes[0]['ip'], num_db, num_doc, 5, False)
        num_node = len(self.nodes)
        for i in range(num_node):
            if i+1 >= num_node:
                target_node = self.nodes[0]
            else:
                target_node = self.nodes[i+1]
            for db in local_dbs:
                replica = Thread(target=self._replicate_db, args=(self.servers[0], self.nodes[i], target_node, db, continuous,))
                replica.start()
                replica.join()

    def _trigger_replication(self, source_server, source_node, target_node, dbs, continuous):
        for db in dbs:
             replica = Thread(target=self._replicate_db, args=(source_server, source_node, target_node, db, continuous,))
             replica.start()
             replica.join()

    def test_two_way_replication(self):
        num_db=1
        num_doc=2
        continuous= True
        dbs = []
        for i in range(len(self.nodes)):
            local_dbs = self._populate_database(self.servers[i], self.nodes[i]['ip'], num_db, num_doc, 0, False)
            dbs.append(local_dbs)
        #a -><- b
        self._trigger_replication(self.servers[0], self.nodes[0], self.nodes[1], dbs[0], continuous)
        self._trigger_replication(self.servers[1], self.nodes[1], self.nodes[0], dbs[1], continuous)
        #a -><- c
        self._trigger_replication(self.servers[0], self.nodes[0], self.nodes[2], dbs[0], continuous)
        self._trigger_replication(self.servers[2], self.nodes[2], self.nodes[0], dbs[2], continuous)
        #b -><- c
        self._trigger_replication(self.servers[1], self.nodes[1], self.nodes[2], dbs[1], continuous)
        self._trigger_replication(self.servers[2], self.nodes[2], self.nodes[1], dbs[2], continuous)

        self.assertEqual(len(self.servers[1].all_dbs())-2, 3) # exclude default _replicator and _user db
        self.assertEqual(len(self.servers[2].all_dbs())-2, 3) # exclude default _replicator and _user db
        self.assertEqual(len(self.servers[0].all_dbs())-2, 3) # exclude default _replicator and _user db
