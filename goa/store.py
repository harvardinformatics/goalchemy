# -*- coding: utf-8 -*-

'''
goa.store  Main class for interacting with the database

Created on  2017-04-25 12:39:14

@author: =
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''
import os
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Column, Table, types, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime

GOALCHEMY_USER      = os.environ.get('GOALCHEMY_USER')
GOALCHEMY_PASSWORD  = os.environ.get('GOALCHEMY_PASSWORD')
GOALCHEMY_DATABASE  = os.environ.get('GOALCHEMY_DATABASE')
GOALCHEMY_HOST      = os.environ.get('GOALCHEMY_HOST','localhost')
GOALCHEMY_DRIVER    = os.environ.get('GOALCHEMY_DRIVER','mysql+mysqldb')

logger = logging.getLogger()


class Store(object):
    '''
    Class that manages interactions with the database.

    Table goa
        with_or_from                Additional identifier(s) to support annotations using certain evidence codes 
                                    (including IEA, IPI, IGI, IMP, IC and ISS evidences).
        
    '''

    def __init__(self,connectstring=None):
        '''
        Create the engine and connection.  Define the jobreport table
        '''

        if connectstring is None:
            connectstring = '%s//%s:%s@%s' % (GOALCHEMY_DRIVER,GOALCHEMY_USER,GOALCHEMY_PASSWORD,GOALCHEMY_HOST)

        # configure Session class with desired options
        self.engine = create_engine(connectstring)
        Session = sessionmaker(bind=self.engine)

        self.session = Session()
        self.metadata = MetaData()
        
        self.tables = {}
        self.tables['goa'] = Table(
            'goa', 
            self.metadata, 
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('db',                            types.String(50)),
            Column('db_object_id',                  types.String(20)),
            Column('db_object_symbol',              types.String(50)),
            Column('qualifier',                     types.String(20)),
            Column('go_id',                         types.String(20)),
            Column('db_reference',                  types.String(50)),
            Column('evidence_code',                 types.String(10)),
            Column('with_or_from',                  types.String(100)), 
            Column('aspect',                        types.String(1)),
            Column('db_object_name',                types.String(50)),
            Column('db_object_synonym',             types.String(500)),
            Column('db_object_type',                types.String(20)),
            Column('taxon',                         types.String(20)),
            Column('organism',                      types.String(50)),
            Column('date',                          types.Date()),
            Column('assigned_by',                   types.String(20)),
            UniqueConstraint('db', 'db_object_id', 'go_id', 'db_reference', name='uix_1'),
        )

        self.metadata.bind = self.engine
        self.connection = self.engine.connect()

    def create(self):
        '''
        Actually creates the database tables.  Be careful
        '''
        self.metadata.create_all(checkfirst=True)
        
    def drop(self):
        '''
        Drop the database table
        '''
        self.metadata.drop_all(checkfirst=True)

    def commit(self):
        '''
        Do a commit
        '''
        self.session.commit()

    def storeGoaRow(self,row):
        '''
        Store a row from the goa file
        '''
        logger.debug('Row is %s' % ' '.join(row))

        # row[13] should be a date of the form YYYYMMDD
        year = int(row[13][0:4])
        month = int(row[13][4:6])
        day = int(row[13][6:8])
        date = datetime(year,month,day)

        i = self.tables['goa'].insert()
        rs = i.execute(
            db=row[0],
            db_object_id=row[1],
            db_object_symbol=row[2],
            qualifier=row[3],
            go_id=row[4],
            db_reference=row[5],
            evidence_code=row[6],
            with_or_from=row[7],
            aspect=row[8],
            db_object_name=row[9],
            db_object_synonym=row[10],
            db_object_type=row[11],
            taxon=row[12],
            date=date,
            assigned_by=row[14],
        )
