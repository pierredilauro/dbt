import copy
import re
import time
import yaml

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Column


connection_cache = {}


class DefaultAdapter:

    ###
    # ADAPTER-SPECIFIC FUNCTIONS -- each of these must be overridden in
    #                               every adapter
    ###
    @contextmanager
    @classmethod
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    def type():
        raise dbt.exceptions.NotImplementedException(
            '`type` is not implemented for this adapter!')

    def date_function(cls):
        raise dbt.exceptions.NotImplementedException(
            '`date_function` is not implemented for this adapter!')

    @classmethod
    def dist_qualifier(cls):
        raise dbt.exceptions.NotImplementedException(
            '`dist_qualifier` is not implemented for this adapter!')

    @classmethod
    def sort_qualifier(cls):
        raise dbt.exceptions.NotImplementedException(
            '`sort_qualifier` is not implemented for this adapter!')

    @classmethod
    def get_status(cls, cursor):
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    @classmethod
    def alter_column_type(cls, profile, schema, table, column_name,
                          new_column_type, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`alter_column_type` is not implemented for this adapter!')

    @classmethod
    def query_for_existing(cls, profile, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`query_for_existing` is not implemented for this adapter!')

    ###
    # FUNCTIONS THAT SHOULD BE ABSTRACT
    ###
    @classmethod
    def drop(cls, profile, relation, relation_type, model_name=None):
        if relation_type == 'view':
            return cls.drop_view(profile, relation, model_name)
        elif relation_type == 'table':
            return cls.drop_table(profile, relation, model_name)
        else:
            raise RuntimeError(
                "Invalid relation_type '{}'"
                .format(relation_type))

    @classmethod
    def drop_view(cls, profile, view, model_name):
        schema = cls.get_default_schema(profile)

        sql = ('drop view if exists "{schema}"."{view}" cascade'
               .format(schema=schema,
                       view=view))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def drop_table(cls, profile, table, model_name):
        schema = cls.get_default_schema(profile)

        sql = ('drop table if exists "{schema}"."{table}" cascade'
               .format(schema=schema,
                       table=table))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def truncate(cls, profile, table, model_name=None):
        schema = cls.get_default_schema(profile)

        sql = ('truncate table "{schema}"."{table}"'
               .format(schema=schema,
                       table=table))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        schema = cls.get_default_schema(profile)

        sql = ('alter table "{schema}"."{from_name}" rename to "{to_name}"'
               .format(schema=schema,
                       from_name=from_name,
                       to_name=to_name))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def execute_model(cls, profile, model):
        parts = re.split(r'-- (DBT_OPERATION .*)', model.get('wrapped_sql'))

        for i, part in enumerate(parts):
            matches = re.match(r'^DBT_OPERATION ({.*})$', part)
            if matches is not None:
                instruction_string = matches.groups()[0]
                instruction = yaml.safe_load(instruction_string)
                function = instruction['function']
                kwargs = instruction['args']

                def call_expand_target_column_types(kwargs):
                    kwargs.update({'profile': profile,
                                   'model_name': model.get('name')})
                    return cls.expand_target_column_types(**kwargs)

                func_map = {
                    'expand_column_types_if_needed':
                    call_expand_target_column_types
                }

                func_map[function](kwargs)
            else:
                connection, cursor = cls.add_query(
                    profile, part, model.get('name'))

        return cls.get_status(cursor)

    @classmethod
    def get_missing_columns(cls, profile,
                            from_schema, from_table,
                            to_schema, to_table,
                            model_name=None):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {col.name: col for col in
                        cls.get_columns_in_table(
                            profile, from_schema, from_table, model_name)}
        to_columns = {col.name: col for col in
                      cls.get_columns_in_table(
                          profile, to_schema, to_table, model_name)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items()
                if col_name in missing_columns]

    @classmethod
    def get_columns_in_table(cls, profile, schema_name, table_name,
                             model_name=None):
        sql = """
        select column_name, data_type, character_maximum_length
        from information_schema.columns
        where table_name = '{table_name}'
        """.format(table_name=table_name).strip()

        if schema_name is not None:
            sql += (" AND table_schema = '{schema_name}'"
                    .format(schema_name=schema_name))

        connection, cursor = cls.add_query(
            profile, sql, model_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size = row
            column = Column(name, data_type, char_size)
            columns.append(column)

        return columns

    @classmethod
    def expand_target_column_types(cls, profile,
                                   temp_table,
                                   to_schema, to_table,
                                   model_name=None):

        reference_columns = {col.name: col for col in
                             cls.get_columns_in_table(
                                 profile, None, temp_table, model_name)}
        target_columns = {col.name: col for col in
                          cls.get_columns_in_table(
                              profile, to_schema, to_table, model_name)}

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                new_type = Column.string_type(reference_column.string_size())
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             target_column.data_type,
                             new_type,
                             to_schema,
                             to_table)

                cls.alter_column_type(profile, to_schema, to_table,
                                      column_name, new_type, model_name)

    ###
    # SANE ANSI SQL DEFAULTS
    ###
    @classmethod
    def get_create_schema_sql(cls, schema):
        return ('create schema if not exists "{schema}"'
                .format(schema=schema))

    @classmethod
    def get_create_table_sql(cls, schema, table, columns, sort, dist):
        fields = ['"{field}" {data_type}'.format(
            field=column.name, data_type=column.data_type
        ) for column in columns]
        fields_csv = ",\n  ".join(fields)
        dist = cls.dist_qualifier(dist)
        sort = cls.sort_qualifier('compound', sort)
        sql = """
        create table if not exists "{schema}"."{table}" (
        {fields}
        )
        {dist} {sort}
        """.format(
            schema=schema,
            table=table,
            fields=fields_csv,
            sort=sort,
            dist=dist).strip()
        return sql

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    @classmethod
    def get_default_schema(cls, profile):
        return profile.get('schema')

    @classmethod
    def get_connection(cls, profile, name=None):
        global connection_cache

        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            name = 'master'

        if connection_cache.get(name):
            return connection_cache.get(name)

        logger.debug('Acquiring new {} connection "{}".'
                     .format(cls.type(), name))

        connection = cls.acquire_connection(profile, name)
        connection_cache[name] = connection

        return cls.get_connection(profile, name)

    @classmethod
    def acquire_connection(cls, profile, name):
        # profile requires some marshalling right now because it includes a
        # wee bit of global config.
        # TODO remove this
        credentials = copy.deepcopy(profile)

        credentials.pop('type', None)
        credentials.pop('threads', None)

        result = {
            'type': cls.type(),
            'name': name,
            'state': 'init',
            'transaction_open': False,
            'handle': None,
            'credentials': credentials
        }

        if dbt.flags.STRICT_MODE:
            validate_connection(result)

        return cls.open_connection(result)

    @classmethod
    def cleanup_connections(cls):
        global connection_cache

        for name, connection in connection_cache.items():
            if connection.get('state') != 'closed':
                logger.debug("Connection '{}' was left open."
                             .format(name))
            else:
                logger.debug("Connection '{}' was properly closed."
                             .format(name))

        connection_cache = {}

    @classmethod
    def reload(cls, connection):
        return cls.get_connection(connection.get('credentials'),
                                  connection.get('name'))

    @classmethod
    def begin(cls, profile, name='master'):
        global connection_cache
        connection = cls.get_connection(profile, name)

        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

        if connection['transaction_open'] is True:
            raise dbt.exceptions.ProgrammingException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.get('name')))

        cls.add_query(connection, 'BEGIN')

        connection['transaction_open'] = True
        connection_cache[name] = connection

        return connection

    @classmethod
    def commit(cls, connection):
        global connection_cache

        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

        connection = cls.reload(connection)

        if connection['transaction_open'] is False:
            raise dbt.exceptions.ProgrammingException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.get('name')))

        logger.debug('On {}: COMMIT'.format(connection.get('name')))
        connection.get('handle').commit()

        connection['transaction_open'] = False
        connection_cache[connection.get('name')] = connection

        return connection

    @classmethod
    def rollback(cls, connection):
        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

        connection = cls.reload(connection)

        if connection['transaction_open'] is False:
            raise dbt.exceptions.ProgrammingException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.get('name')))

        logger.debug('On {}: ROLLBACK'.format(connection.get('name')))
        connection.get('handle').rollback()

        connection['transaction_open'] = False
        connection_cache[connection.get('name')] = connection

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

        connection = cls.reload(connection)

        connection.get('handle').close()

        connection['state'] = 'closed'
        connection_cache[connection.get('name')] = connection

        return connection

    @classmethod
    def add_query(cls, profile, sql, model_name=None):
        connection = cls.get_connection(profile, model_name)
        connection_name = connection.get('name')

        logger.debug('Using {} connection "{}".'
                     .format(cls.type(), connection_name))

        with cls.exception_handler(profile, sql, model_name, connection_name):
            logger.debug('On {}: {}'.format(connection_name, sql))
            pre = time.time()

            cursor = connection.get('handle').cursor()
            cursor.execute(sql)

            logger.debug("SQL status: %s in %0.2f seconds",
                         cls.get_status(cursor), (time.time() - pre))

            return connection, cursor

    @classmethod
    def execute_one(cls, profile, sql, model_name=None):
        cls.get_connection(profile, model_name)

        return cls.add_query(profile, sql, model_name)

    @classmethod
    def execute_all(cls, profile, sqls, model_name=None):
        connection = cls.get_connection(profile, model_name)

        if len(sqls) == 0:
            return connection

        for i, sql in enumerate(sqls):
            connection, _ = cls.add_query(profile, sql, model_name)

        return connection

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".'.format(schema))
        sql = cls.get_create_schema_sql(schema)
        return cls.add_query(profile, sql, model_name)

    @classmethod
    def create_table(cls, profile, schema, table, columns, sort, dist,
                     model_name=None):
        logger.debug('Creating table "%s".'.format(schema, table))
        sql = cls.get_create_table_sql(schema, table, columns, sort, dist)
        return cls.add_query(profile, sql, model_name)

    @classmethod
    def table_exists(cls, profile, schema, table, model_name=None):
        tables = cls.query_for_existing(profile, schema, model_name)
        exists = tables.get(table) is not None
        return exists
