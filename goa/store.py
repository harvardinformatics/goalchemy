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
from sqlalchemy import MetaData, Column, Table, types, UniqueConstraint
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime

GOALCHEMY_USER      = os.environ.get('GOALCHEMY_USER')
GOALCHEMY_PASSWORD  = os.environ.get('GOALCHEMY_PASSWORD')
GOALCHEMY_DATABASE  = os.environ.get('GOALCHEMY_DATABASE')
GOALCHEMY_HOST      = os.environ.get('GOALCHEMY_HOST', 'localhost')
GOALCHEMY_DRIVER    = os.environ.get('GOALCHEMY_DRIVER', 'mysql+mysqldb')

logger = logging.getLogger()


class Store(object):
    '''
    Class that manages interactions with the database.

    Table goa
        with_or_from                Additional identifier(s) to support annotations using certain evidence codes
                                    (including IEA, IPI, IGI, IMP, IC and ISS evidences).
    '''

    def __init__(self, connectstring=None):
        '''
        Create the engine and connection.  Define the jobreport table
        '''

        if connectstring is None:
            connectstring = '%s://%s:%s@%s' % (GOALCHEMY_DRIVER, GOALCHEMY_USER, GOALCHEMY_PASSWORD, GOALCHEMY_HOST)

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
        self.tables['go_term'] = Table(
            'go_term',
            self.metadata,
            Column('go_id',                         types.String(20), primary_key=True),
            Column('term',                          types.String(200)),
        )
        self.tables['alias'] = Table(
            'alias',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('authority',                     types.String(50)),
            Column('accession',                     types.String(100)),
            Column('alias',                         types.String(100)),
            Column('source',                        types.String(100)),
            UniqueConstraint('alias', 'source', name='uix_1'),
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

    def storeGoaRow(self, row):
        '''
        Store a row from the goa file (GAF 2.1)
        '''
        logger.debug('Row is %s' % ' '.join(row))

        # row[13] should be a date of the form YYYYMMDD
        year = int(row[13][0:4])
        month = int(row[13][4:6])
        day = int(row[13][6:8])
        date = datetime(year, month, day)

        i = self.tables['goa'].insert()
        i.execute(
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

    def searchByIdListFile(self, listfilename):
        '''
        Searches by ID list provided by a file.   Very MySQL specific
        '''

        # Create an in-memory temp table using a hash of the filename
        import hashlib
        m = hashlib.md5()
        m.update(listfilename)
        tablename = 'tmp_%s' % m.hexdigest()[:11]
        sql = 'create table %s (source varchar(50), id varchar(100)) engine=memory' % tablename
        self.session.execute(sql)
        self.session.commit()

        # Load from the local data file
        sql = "load data local infile '%s' into table %s" % (listfilename, tablename)
        self.session.execute(sql)
        self.session.commit()

        results = []

        try:
            # Join against the GOA table
            sql = """
               select
                    distinct t.id, g.db_object_symbol as goa_symbol, group_concat(distinct gt.term separator ';') as go_terms
                from
                    {tmptable} t
                        inner join alias a on (t.id = a.alias and t.source = a.source)
                        inner join goa g on (g.db = a.authority and g.db_object_id = a.accession)
                        inner join go_term gt on g.go_id = gt.go_id
                group by t.id, goa_symbol
            """.format(tmptable=tablename).translate(None, "\n")

            print "SQL is:\n%s\n" % sql
            self.session.execute('SET SESSION group_concat_max_len = 10000000')
            handle = self.session.execute(sql)
            results = handle.fetchall()
        except Exception as e:
            print 'Error selecting goa data from id list %s.' % str(e)

        return results

    def initBioSqlAliases(self, biodatabase_id=1):
        """
        Initialize the alias table using the BioSql table (I know.  It is supposed to be there.)
        """
        sql = """
            insert into alias (authority, accession, alias, source)
            select 'UniProtKB', be.accession, be.accession, 'UniProtKB'
            from bioentry be
            where be.biodatabase_id = :dbid
        """.translate(None, "\n")
        self.session.execute(sql, {"dbid": biodatabase_id})
        self.session.commit()

        sql = """
            insert into alias (authority, accession, alias, source)
            select 'UniProtKB', be.accession, concat('UniRef90_',be.accession), 'UniRef90'
            from bioentry be
            where be.biodatabase_id = :dbid
        """.translate(None, "\n")
        self.session.execute(sql, {"dbid": biodatabase_id})
        self.session.commit()

        sql = """
            insert into alias (authority, accession, alias, source)
            select 'UniProtKB', be.accession, concat('UniRef100_',be.accession), 'UniRef100'
            from bioentry be
            where be.biodatabase_id = :dbid
        """.translate(None, "\n")
        self.session.execute(sql, {"dbid": biodatabase_id})
        self.session.commit()


