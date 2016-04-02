import argparse
import sys
from configparser import ConfigParser

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session


class OracleDBManager(object):
    def __init__(
            self,
            connection_string: str
    ) -> None:
        self.connection_string = connection_string

        self.engine = None
        self.meta = None
        self.session_factory = None

        self.connect()

    def connect(self, debug=False) -> None:
        if self.engine is None:

            if not debug:
                self.engine = create_engine(
                    self.connection_string,
                    echo=False
                )
            else:
                self.engine = create_engine(
                    self.connection_string,
                    echo=True
                )
        if self.meta is None:
            self.meta = MetaData()
            self.meta.bind = self.engine
        if self.session_factory is None:
            self.session_factory = sessionmaker(bind=self.engine)

    def create_session(self) -> Session:
        self.connect()
        return self.session_factory()

    def reverse_table(self, table_name: str) -> Table:
        self.connect()
        if table_name in self.meta.tables.keys():
            return self.meta.tables[table_name]
        return Table(table_name, self.meta, autoload=True)


def create_HBase_tables(table, conn='sqlite:///HBase.db'):
    db = OracleDBManager(
        conn
    )

    MetaEntity = db.reverse_table(table)

    session = db.create_session()

    result = session.query(MetaEntity).all()

    session.close()

    s = []
    for each in result:
        d = each._asdict()
        s.append(
            'create table \'' + d['Entity_Name'] + '\', \'details\''
        )
    return s


def create_HBase_data(table, conn='sqlite:///HBase.db'):
    db = OracleDBManager(
        conn
    )

    MetaAttributes = db.reverse_table(table)

    session = db.create_session()

    result = session.query(MetaAttributes).order_by('Attr_Nr').all()

    session.close()

    s = []
    for each in result:
        d = each._asdict()
        s.append(
            'put \'' + d['Table_Name'] + '\', \'1\', \'details:' + d['Attr_Name'] + '\', \'1\''
        )

    return s


config = ConfigParser()
config.read('config.ini')

parser = argparse.ArgumentParser(description='Generate DDL to load data from any RDBMS to HBase or Cassandra')

parser.add_argument('--hbase', dest='hbase', action='store_true',
                    default=False,
                    help='Generate DDL for HBase')

parser.add_argument('--cassandra', dest='cassandra', action='store_true',
                    default=False,
                    help='Generate DDL for Cassandra')

# parser.add_argument('--debug', dest='debug', action='store_true',
#                    default=False,
#                    help='Show verbose information')

args = vars(parser.parse_args())

# DEBUG = config['DEFAULT']['DEBUG']

if len(sys.argv) == 1:
    print("Error: no options passed")
    parser.print_help()

elif args['cassandra'] and args['hbase']:
    print("Error: Only one of HBase or Cassandra is allowed at a time")
    parser.print_help()

elif args['hbase']:
    result = create_HBase_tables(
        config['TABLES']['ENTITY'],
        config['DATABASE']['CONNECTION_STRING']
    )

    result = create_HBase_data(
        config['TABLES']['ATTRIBUTES'],
        config['DATABASE']['CONNECTION_STRING']
    )
elif args['cassandra']:
    pass
