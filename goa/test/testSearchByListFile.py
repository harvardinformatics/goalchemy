# -*- coding: utf-8 -*-

'''
Test search by list file functionality


@author: Aaron Kitzmiller
@copyright: 2017 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''

import unittest, os
import tempfile
from sqlalchemy import create_engine

from goa import Store
from goa.loader import loadGoaFile

GOALCHEMY_TEST_DRIVER 		= os.environ.get('GOALCHEMY_TEST_DRIVER', 'mysql+mysqldb')
GOALCHEMY_TEST_USER 		= os.environ.get('GOALCHEMY_TEST_USER')
GOALCHEMY_TEST_PASSWORD 	= os.environ.get('GOALCHEMY_TEST_PASSWORD')
GOALCHEMY_TEST_DATABASE 	= os.environ.get('GOALCHEMY_TEST_DATABASE','goalchemytest')
GOALCHEMY_TEST_HOST 		= os.environ.get('GOALCHEMY_TEST_HOST','localhost')

DATA_FILE = os.path.join(os.path.dirname(__file__),'goa_uniprot_all.gaf.sample')


def initdb():
    engine = create_engine(
        '%s://%s:%s@%s' % (GOALCHEMY_TEST_DRIVER, GOALCHEMY_TEST_USER, GOALCHEMY_TEST_PASSWORD, GOALCHEMY_TEST_HOST))
    connection = engine.connect()
    try:
        connection.execute('drop database %s' % GOALCHEMY_TEST_DATABASE)
    except Exception:
        pass

    connection.execute('create database %s' % GOALCHEMY_TEST_DATABASE)
    engine.dispose()


def destroydb():
    engine = create_engine(
        '%s://%s:%s@%s' % (GOALCHEMY_TEST_DRIVER, GOALCHEMY_TEST_USER, GOALCHEMY_TEST_PASSWORD, GOALCHEMY_TEST_HOST))
    connection = engine.connect()
    try:
        connection.execute('drop database %s' % GOALCHEMY_TEST_DATABASE)
    except Exception:
        pass
    engine.dispose()


class Test(unittest.TestCase):

    def setUp(self):
        try:
            destroydb()
        except Exception:
            pass
        initdb()
        self.store = Store('%s://%s:%s@%s/%s?local_infile=1' % (GOALCHEMY_TEST_DRIVER, GOALCHEMY_TEST_USER, GOALCHEMY_TEST_PASSWORD, GOALCHEMY_TEST_HOST, GOALCHEMY_TEST_DATABASE))
        self.store.create()

    def tearDown(self):
        del self.store
        destroydb()

    def testSearchByListFile(self):
        ''' 
        Load a list of ids and get the gene symbols back
        '''
        loadGoaFile(self.store,DATA_FILE,100)
        idlist = ['A0A003','A0A009']
        tf = tempfile.NamedTemporaryFile(delete=False)
        tfname = tf.name

        tf.write('\n'.join(set(idlist)))
        tf.close()

        result = sorted(self.store.searchByIdListFile(tfname),key=lambda data: data[0])
        self.assertTrue(','.join(result[0]) == 'A0A003,moeE5', 'Bad data: %s' % str(result[0]))
        self.assertTrue(','.join(result[1]) == 'A0A009,moeM5', 'Bad data: %s' % str(result[1]))
