#!/usr/bin/python

import uuid
import os, sys
import json
from snap import common
from mercury.dataload import DataStore
from sqlalchemy.sql import text
import constants


class ObjectFactory(object):
    @classmethod
    def create_subscription_fact(cls, db_svc, **kwargs):
        SubscriptionFact = db_svc.Base.classes.fact_subscription_events
        return SubscriptionFact(**kwargs)


class FileStore(DataStore):
    def __init__(self, service_object_registry, *channels, **kwargs):
        DataStore.__init__(self, service_object_registry, *channels, **kwargs)
        kwreader = common.KeywordArgReader('filename')
        kwreader.read(**kwargs)
        self.filename = kwreader.get_value('filename')


    def write(self, records, **kwargs):
        with open(self.filename, 'a') as f:
            for record in records:
                f.write(record)
                f.write('\n')


class RedshiftS3Manifest(object):
    def __init__(self):
        self.s3_entries = []

    def add_s3_key(self, s3_key, is_mandatory=False):
        self.s3_entries.append({
            'url': s3_key.uri,
            'mandatory': is_mandatory
        })

    def build(self):
        return {
            'entries': self.s3_entries
        }

    def __str__(self):
        return json.dumps(self.build())


class RedshiftS3Context(object):
    def __init__(self, db_schema, aws_access_key_id, aws_secret_key, **kwargs):   
        self.schema = db_schema     
        self.access_key_id = aws_access_key_id
        self.secret_key = aws_secret_key

    
    def generate_copy_statement_manifest(self, table_name, manifest_key, **kwargs):
        return "copy %s from '%s' credentials '%s' %s;" % (table_name, manifest_key, self.credentials(), "manifest delimiter ',';")
    

    def generate_copy_statement(self, table_name, datafile_s3_key, data_format, **kwargs):

        manifest_mode = kwargs.get('use_manifest', False)
        # NOTE: if we call this method with the keyword arg use_manifest=True,
        # then the datafile_s3_key must point to the JSON manifest file,
        # not the source data file. (The manifest file itself points to 1-N data files)
                
        if data_format == 'json':
            statement_tpl = """
            COPY {schema}.{table}
            FROM '{datafile}' 
            CREDENTIALS 'aws_access_key_id={key_id};aws_secret_access_key={key}' 
            FORMAT AS JSON 'auto'"""

            params = dict(
                schema=self.schema,
                table=table_name,
                datafile=datafile_s3_key.uri,
                key=self.secret_key,
                key_id=self.access_key_id)

            if manifest_mode:
                statement_tpl = statement_tpl + ' MANIFEST' # do not omit the leading space char
            
            statement_tpl = statement_tpl + ';'

            return statement_tpl.format(**params)

        elif data_format == 'csv':
            delimiter_char = kwargs.get('delimiter', ',')
            statement_tpl = """
            COPY {table} FROM '{datafile}'
            ACCESS_KEY_ID '{key}' SECRET_ACCESS_KEY '{key_id}'
            CREDENTIALS {creds}
            FORMAT CSV
            DELIMITER '{delimiter}'
            """
            
            if manifest_mode:
                statement_tpl.append(' MANIFEST')

            return statement_tpl.format(table=table_name,
                                        datafile=datafile_s3_key.uri,
                                        key=self.secret_key,
                                        key_id=self.access_key_id,
                                        delimiter=delimiter_char)

        else:
            raise Exception('format "%s" not supported.' % data_format)

    
    def export_records(self, db_connection, query, output_file_prefix, **kwargs):
        pass
        #unload_statement = self.generate_unload_statement(query, output_file_prefix, **kwargs)        
        #db_connection.execute(unload_statement)


    def import_records(self, db_connection, table_name, s3_object_key, data_format, **kwargs):
        copy_stmt = self.generate_copy_statement(table_name, s3_object_key, data_format, **kwargs)        
        db_connection.execute(copy_stmt)
        return copy_stmt



class RedshiftDatastore(DataStore):
    def __init__(self, service_object_registry, *channels, **kwargs):
        DataStore.__init__(self, service_object_registry, *channels, **kwargs)


    def detect_channel(self, record):       
        return None

    def _generate_temp_filename(self, channel_id):
        return 'eos_tmpdata_%s_%s.json' % (uuid.uuid4(), channel_id)


    def upload_channel_data(self, channel_id, **kwargs):
        services = self.service_object_registry
        bucket_name = 'c3-staging-dexter-temp'
        channel_file_tbl = kwargs['channel_files']
        channel_id = kwargs['channel']
        s3_svc = services.lookup('s3')
        file_to_upload = channel_file_tbl[channel_id]
        try:
            s3key = s3_svc.upload_object(file_to_upload, bucket_name)
            return s3key
        finally:
            os.remove(file_to_upload)

        
    def write_lookup_address_state(self, records, **kwargs):
        bucket_name = 'c3-staging-dexter-temp'
        services = self.service_object_registry
        redshift_svc = services.lookup('redshift')
        s3_svc = services.lookup('s3')
        channel_id = kwargs['channel']
        s3_key = self.upload_channel_data(channel_id, **kwargs)

        rs3ctx = RedshiftS3Context(redshift_svc.schema,
                                   s3_svc.aws_access_key_id,
                                   s3_svc.aws_secret_access_key)

        table_name = channel_id

        with redshift_svc.connect() as cxn:
            tx = cxn.begin()
            print('### loading records to table %s from S3 object %s...' % (table_name, s3_key.uri))
            rs3ctx.import_records(cxn,
                                  table_name,
                                  s3_key,
                                  'json')
            tx.commit()
       

    def bulk_write(self, records, **kwargs):
        redshift_svc = self.service_object_registry.lookup('redshift')
        s3_svc = self.service_object_registry.lookup('s3')
        channel_id = kwargs['channel']
        s3_key = self.upload_channel_data(channel_id, **kwargs)

        rs3ctx = RedshiftS3Context(redshift_svc.schema,
                                   s3_svc.aws_access_key_id,
                                   s3_svc.aws_secret_access_key)

        table_name = channel_id
        with redshift_svc.connect() as cxn:
            tx = cxn.begin()
            print('### loading records into table %s from S3 object %s...' % (table_name, s3_key.uri))
            rs3ctx.import_records(cxn,
                                  table_name,
                                  s3_key,
                                  'json')
            tx.commit()


    def write_default_channel(self, records, **kwargs):
        self.bulk_write(records, **kwargs)


    def flush_channel_buffer_to_files(self, channel_buffer):
        channel_file_data = {}
        for channel_id, records in channel_buffer.items():
            filename = self._generate_temp_filename(channel_id)
            temp_file = os.path.join('/tmp', filename)
            channel_file_data[channel_id] = temp_file

            with open(temp_file, 'w+') as f:
                for record in records:
                    f.write(json.dumps(record))
                    f.write('\n')

        return channel_file_data


    def write(self, records, **kwargs):
        print('### writing %s records to datastore...' % len(records), file=sys.stderr)
        channel_write_buffer = {}
        for raw_record in records:
            record = json.loads(raw_record)
            if kwargs.get('channel'):
                channel_id = kwargs['channel']
            else:
                channel_id = self.detect_channel(record)
                
            if channel_id:
                if not channel_write_buffer.get(channel_id):
                    channel_write_buffer[channel_id] = []
                channel_write_buffer[channel_id].append(record)
            else:
                print('### This DataStore only accepts channel-mode writes. Skipping records.', file=sys.stderr)
                break

        channel_filedata = self.flush_channel_buffer_to_files(channel_write_buffer)
        kwargs.update(channel_files=channel_filedata)

        for channel_id, records in channel_write_buffer.items():
            kwargs['channel'] = channel_id
            writefunc = self.get_channel_write_function(channel_id)
            writefunc(records, **kwargs)


def copy_fields_from(source_dict, *fields):
    output_dict = {}
    for field in fields:
        output_dict[field] = source_dict[field]
    
    return output_dict


class DimensionIDLookupService(object):
    def __init__(self, **kwargs):
        self.tables = {}

    def load_dim_table(self, table_name, schema, connection):
        if self.tables.get(table_name):
            return

        value_lookup = {}
        load_query = 'SELECT id, value FROM %s.%s' % (schema, table_name)
        statement = text(load_query)
        resultset = connection.execute(statement, schema_name=schema)
        for record in resultset:
            value_lookup[record['value']] = record['id']

        self.tables[table_name]= value_lookup


    def get_id_for_value(self, value, dim_table_name, schema, connection):
        self.load_dim_table(dim_table_name, schema, connection)
        return self.tables[dim_table_name].get(value)


class PostgresOLAPDatastore(DataStore):
    def __init__(self, service_object_registry, **kwargs):
        DataStore.__init__(self, service_object_registry, **kwargs)
        self.target_schema = kwargs['target_schema']
        self.fact_table = kwargs['fact_table']
        self.dim_tables = kwargs['dim_tables']
        self.dim_lookup = DimensionIDLookupService()

        postgres_svc = self.service_object_registry.lookup('postgres')
        with postgres_svc.connect() as connection:
            for tbl in self.dim_tables:
                self.dim_lookup.load_dim_table(tbl, self.target_schema, connection)


    def prepare_fact_record(self, input_record):
        # this will fail on the insert, of course
        output_record = copy_fields_from(input_record, 'first_name', 'last_name', 'street_address', 'zip_code', 'email')
        output_record['id'] = uuid.uuid4()

        postgres_svc = self.service_object_registry.lookup('postgres')
        with postgres_svc.connect() as connection:
            state_id = self.dim_lookup.get_id_for_value(input_record['state'], 'dim_state', self.target_schema, connection)
            output_record['state_id'] = state_id

            start_date_array = input_record['start_date']

            year_val = start_date_array[constants.YEAR_INDEX]
            year_id = self.dim_lookup.get_id_for_value(year_val, 'dim_date_year', self.target_schema, connection)
            output_record['date_year_id'] = year_id

            month_val = start_date_array[constants.MONTH_INDEX]
            month_id = self.dim_lookup.get_id_for_value(month_val, 'dim_date_month', self.target_schema, connection)
            output_record['date_month_id'] = month_id

            day_val = start_date_array[constants.DAY_INDEX]
            day_id = self.dim_lookup.get_id_for_value(day_val, 'dim_date_day', self.target_schema, connection)
            output_record['date_day_id'] = day_id

            event_type_id = self.dim_lookup.get_id_for_value('start', 'dim_event_type', self.target_schema, connection)
            output_record['event_type_id'] = event_type_id            

 
        print(output_record)
        return output_record


    def write(self, records, **kwargs):
        postgres_svc = self.service_object_registry.lookup('postgres')        

        with postgres_svc.txn_scope() as session:
            for raw_record in records:
                #print('### datastore reading input record: %s' % raw_record, file=sys.stderr)
                record = json.loads(raw_record)
                db_record = self.prepare_fact_record(record)
                fact = ObjectFactory.create_subscription_fact(postgres_svc, **db_record)
                #session.add(fact)

                print('>>> wrote record to database: %s' % db_record, file=sys.stderr)
