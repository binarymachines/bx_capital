#!/usr/bin/env python

'''
Usage:
    mkpgpass (--append | --create) --file <pgpass_file> 

Options:
    -a --append   add entry to existing pgpass file
    -c --create   create new pgpass file
'''

APPEND_MODE = '--append'
CREATE_MODE = '--create'

import os
import getpass
import docopt

PROMPTS = {
    'host': 'PostgreSQL host: ',
    'port': 'PostgreSQL port [5432]: ',
    'database': 'database name: ',
    'username': 'username: '
}


LINE_TEMPLATE = '{host}:{port}:{database}:{username}:{password}'


def main(args):
    print(args)

    params = {}
    params['host'] = input(PROMPTS['host'])
    params['port'] = input(PROMPTS['port']) or 5432
    params['database'] = input(PROMPTS['database'])
    params['username'] = input(PROMPTS['username'])
    params['password'] = getpass.getpass('password: ')
    
    new_entry = LINE_TEMPLATE.format(**params)

    target_filename = args['<pgpass_file>']

    if args[CREATE_MODE]:
        if os.path.isfile(target_filename):
            ('The target pgpass file "%s" already exists.')
            exit()
        else:
            with open(target_filename, 'w') as f:
                f.write(new_entry)
                f.write('\n')
    
    if args[APPEND_MODE]:
        with open(target_filename, 'a+') as f:
            f.write(new_entry)
            f.write('\n')
    

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)