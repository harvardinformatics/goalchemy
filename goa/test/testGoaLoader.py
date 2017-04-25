# -*- coding: utf-8 -*-

'''
test the loader

Created on  2017-04-25 15:12:00

@author: =
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''

import unittest, os
import subprocess
from sqlalchemy import select,func, create_engine
from goa import Store
from goa.loader import loadGoaFile

GOALCHEMY_TEST_DRIVER = os.environ.get('GOALCHEMY_TEST_DRIVER', 'mysql+mysqldb')
GOALCHEMY_TEST_USER = os.environ.get('GOALCHEMY_TEST_USER')
GOALCHEMY_TEST_PASSWORD = os.environ.get('GOALCHEMY_TEST_PASSWORD')
GOALCHEMY_TEST_DATABASE = os.environ.get('GOALCHEMY_TEST_DATABASE','goalchemytest')
GOALCHEMY_TEST_HOST = os.environ.get('GOALCHEMY_TEST_HOST','localhost')

DATA_FILE = os.path.join(os.path.dirname(__file__),'goa_uniprot_all.gaf.sample')


def runcmd(cmd):
    '''
    Execute a command and return stdout, stderr, and the return code
    '''
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutstr, stderrstr = proc.communicate()
    return (proc.returncode, stdoutstr, stderrstr)


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
        self.store = Store('%s://%s:%s@%s/%s' % (GOALCHEMY_TEST_DRIVER, GOALCHEMY_TEST_USER, GOALCHEMY_TEST_PASSWORD, GOALCHEMY_TEST_HOST, GOALCHEMY_TEST_DATABASE))
        self.store.create()

    def tearDown(self):
        del self.store
        destroydb()

    def testPeptideLoad(self):
        '''
        Test loadPeptideDataFile function
        '''
        loadGoaFile(self.store, DATA_FILE, 100)
        s = select([func.count(self.store.tables['goa'].c.id)])
        rs = s.execute()
        rowcount = rs.first()[0]
        self.assertTrue(42 == rowcount,'Incorrect row count %d' % rowcount)
