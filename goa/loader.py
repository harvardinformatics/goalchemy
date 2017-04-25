#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
UniProt GOA loader

Created on  2017-04-25 13:06:50

@author: Aaron Kitzmiller <aaron_kitzmiller@harvard.edu>
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''

import os, sys, traceback
import logging
from goa import Store
from goa import __version__ as version

from argparse import ArgumentParser, RawDescriptionHelpFormatter

logging.basicConfig(format='%(asctime)s: %(message)s',level=logging.ERROR)
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ.get('GOALCHEMY_LOGLEVEL','ERROR')))


def initArgs():
    '''
    Setup arguments with parameterdef, check envs, parse commandline, return args
    '''

    parameterdefs = [
        {
            'name'      : 'GOALCHEMY_LOGLEVEL',
            'switches'  : ['--loglevel'],
            'required'  : False,
            'help'      : 'Log level (e.g. DEBUG, INFO)',
            'default'   : 'INFO',
        },
        {
            'name'      : 'GOALCHEMY_DRIVER',
            'switches'  : ['--driver'],
            'required'  : False,
            'help'      : 'Database connection driver (e.g. mysql+mysqldb).  See SQLAlchemy docs.',
            'default'   : 'mysql+mysqldb',
        },
        {
            'name'      : 'GOALCHEMY_USER',
            'switches'  : ['--user'],
            'required'  : False,
            'help'      : 'Database user',            
        },
        {
            'name'      : 'GOALCHEMY_PASSWORD',
            'switches'  : ['--password'],
            'required'  : False,
            'help'      : 'Database password',
        },
        {
            'name'      : 'GOALCHEMY_HOST',
            'switches'  : ['--host'],
            'required'  : False,
            'help'      : 'Database hostname',
        },
        {
            'name'      : 'GOALCHEMY_DATABASE',
            'switches'  : ['--database'],
            'required'  : False,
            'help'      : 'Database name',
        },
        {
            'name'      : 'GOALCHEMY_CREATE',
            'switches'  : ['--create'],
            'required'  : False,
            'help'      : 'Creates the database tables.  The database must exist.  FILE argument will be ignored and no load will occur.',
            'action'    : 'store_true',
        },
        {
            'name'      : 'GOALCHEMY_COMMIT_COUNT',
            'switches'  : ['--commit-count'],
            'required'  : False,
            'help'      : 'Number of rows between database commits.',
            'default'   : '100',
        },
    ]
        
    # Check for environment variable values
    # Set to 'default' if they are found
    for parameterdef in parameterdefs:
        if os.environ.get(parameterdef['name'],None) is not None:
            parameterdef['default'] = os.environ.get(parameterdef['name'])
            
    # Setup argument parser
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version=version)
    parser.add_argument('--file',required=False, help='Input data file')
    
    # Use the parameterdefs for the ArgumentParser
    for parameterdef in parameterdefs:
        switches = parameterdef.pop('switches')
        if not isinstance(switches, list):
            switches = [switches]
            
        # Gotta take it off for add_argument
        name = parameterdef.pop('name')
        parameterdef['dest'] = name
        if 'default' in parameterdef:
            parameterdef['help'] += '  [default: %s]' % parameterdef['default']
        parser.add_argument(*switches,**parameterdef)
        
        # Gotta put it back on for later
        parameterdef['name'] = name
        
    args = parser.parse_args()
    return args


def main():
    args = initArgs()

    if args.GOALCHEMY_LOGLEVEL:
        logger.setLevel(logging.getLevelName(args.GOALCHEMY_LOGLEVEL))

    driver      = args.GOALCHEMY_DRIVER
    user        = args.GOALCHEMY_USER
    password    = args.GOALCHEMY_PASSWORD
    host        = args.GOALCHEMY_HOST
    database    = args.GOALCHEMY_DATABASE
    filename    = args.file
    commitcount = int(args.GOALCHEMY_COMMIT_COUNT)

    try:

        store = Store('%s://%s:%s@%s/%s' % (
            driver, 
            user, 
            password, 
            host, 
            database,
        ))
        if args.GOALCHEMY_CREATE:
            store.create()
            logger.info('Created database tables.')
        else:
            if not os.path.exists(filename):
                raise Exception('File %s does not exist.' % filename)

            savedcount = 0
            errors = []
            with open(filename,'r') as f:
                for line in f:
                    line = line.strip()
                    if line == '' or line.startswith('!'):
                        continue
                    row = line.split('\t')

                    try:
                        store.storeGoaRow(row)
                        savedcount += 1
                    except Exception as e:
                        errors.append(str(e))
                        logger.debug('Error loading row: %s\n%s\n%s' % (str(e),line,traceback.format_exc()))

                    if savedcount > 0 and savedcount % commitcount == 0:
                        store.commit()
                        logger.info('Saved %d records' % savedcount)

            store.commit()
            logger.info('%d records saved' % savedcount)
            if len(errors) > 0:
                logger.error('Errors occurred during loading:\n%s' % '\n'.join(errors))

    except Exception as e:
        print '%s:\n%s' % (str(e), traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())
