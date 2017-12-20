# coding: utf-8
from __future__ import unicode_literals
from collections import OrderedDict
import sqlite3
import os


class Connection(object):
    def __init__(self, name='sqlite.db'):
        self.db_name = name
        self.connect()
        self._cursor = None

    def __enter__(self):
        self.cursor()
        return self

    def __exit__(self, type, value, trace):
        self.commit()

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)

    def clean(self):
        self.close()
        if os.path.isfile(self.db_name):
            os.remove(self.db_name)

    def cursor(self):
        self._cursor = self.conn.cursor()
        return self._cursor

    def execute(self, content):
        try:
            sql = self._cursor.execute(content)
            result = sql.fetchall()
            return result
        except Exception as e:
            print content
            raise e

    def commit(self):
        self.conn.commit()

    def __del__(self):
        self.close()

    def close(self):
        self.conn.close()


connection = Connection()


class QuerySet(object):
    def __init__(self, model):
        self.model = model
        self.filter_kwargs = {}
        self.exclude_kwargs = {}
        self.select_args = set()

    def filter(self, **kwargs):
        clone = self._clone()
        clone.filter_kwargs.update(kwargs)
        return clone

    def exclude(self, **kwargs):
        clone = self._clone()
        clone.exclude_kwargs.update(kwargs)
        return clone

    def hint(self):
        select_sql = self._value_sql()
        where_sql = self._filter_or_exclude_sql()
        execute_sql = 'select {} from {} where {}'.format(
            select_sql, self.model.meta['table_name'], where_sql,
        )
        with connection as conn:
            raw_data = conn.execute(execute_sql)
        return [self.model(**dict(zip(self.select_args, data))) for data in raw_data]

    def _value_sql(self):
        if not self.select_args:
            self.select_args.update(self.model.fields)
        return ', '.join(['{}.{}'.format(self.model.meta['table_name'], i) for i in self.select_args])

    def _filter_or_exclude_sql(self):
        sql = []
        filter_sql = self.__filter_or_exclude_sql(False, **self.filter_kwargs)
        if filter_sql:
            sql.append(filter_sql)
        filter_sql = self.__filter_or_exclude_sql(True, **self.exclude_kwargs)
        if filter_sql:
            sql.append(filter_sql)
        return ' AND '.join(sql)

    def __filter_or_exclude_sql(self, negate, **kwargs):
        field_sql = []
        for field, value in kwargs.items():
            field, lookup = self.__where_lookup(field)
            field_sql.append(self.__field_lookup_sql(negate, field, lookup, value))
        return ' AND '.join(field_sql)

    def __where_lookup(self, field):
        LOOKUP_MAP = {
            '': '=',
            'gte': '>=',
            'gt': '>',
            'lte': '<=',
            'lt': '<',
            'contains': 'LIKE',
            'in': 'IN',
        }
        if '__' in field:
            _field, _lookup = field.split('__')
            if _lookup not in LOOKUP_MAP:
                _field = field
        else:
            _field, _lookup = field, ''
        field = _field
        lookup = LOOKUP_MAP.get(_lookup, LOOKUP_MAP[''])
        return field, lookup

    def __field_lookup_sql(self, negate, field, lookup, value):
        _negate = {False: '', True: 'NOT '}[negate]
        _field = '{}.{}'.format(self.model.meta['table_name'], field)
        if type(value) in (list, tuple):
            if getattr(self.model, field).field_type == 'varchar':
                _value = ['"{}"'.format(i) for i in value]
                _value = '({})'.format(', '.join(_value))
        else:
            if getattr(self.model, field).field_type == 'varchar':
                _value = '"{}"'.format(value)
            else:
                _value = value
        sql = '{negate}{field} {lookup} {value}'.format(
            negate=_negate, field=_field, lookup=lookup, value=_value
        )
        return sql

    def _clone(self):
        clone = self.__class__(self.model)
        clone.filter_kwargs = dict(self.filter_kwargs)
        clone.exclude_kwargs = dict(self.exclude_kwargs)
        return clone


class ValidateException(Exception):
    pass


class FieldNotExists(Exception):
    pass


class BaseField(object):
    name = None
    field_type = None

    def __init__(self, primary=False, verbose_name=None, output=None):
        self.primary = primary
        self.verbose_name = verbose_name or self.name
        if not output:
            output = verbose_name
        self.output = output

    def migrate_sql(self):
        return '{} {}{}'.format(self._migrate_field_name(),
                                 self._migrate_field_type(),
                                 self._migrate_primary_key())

    def _migrate_field_name(self):
        return self.name

    def _migrate_field_type(self):
        return self.field_type

    def _migrate_primary_key(self):
        return ' primary key' if self.primary else ''

    def validate(self, value):
        return value

    def value_to_db(self, value):
        return str(value)


class CharField(BaseField):
    field_type = 'varchar'

    def __init__(self, max_size, *args, **kwargs):
        self.max_size = int(max_size)
        super(CharField, self).__init__(*args, **kwargs)

    def _migrate_field_type(self):
        return '{}({})'.format(self.field_type, self.max_size)

    def validate(self, value):
        if len(value) > self.max_size:
            raise ValidateException('Your CharField {} settings max size is {}. Now char size is {}.'.format(self.name, self.max_size, len(value)))
        return value

    def value_to_db(self, value):
        return '"{}"'.format(value)


class IntegerField(BaseField):
    field_type = 'integer'

    def validate(self, value):
        return int(value or 0)


class MetaModel(type):
    def __new__(cls, name, bases, attrs):
        fields = OrderedDict()
        has_primary_key = False
        for key in attrs:
            if key == '__module__':
                continue
            if isinstance(attrs[key], BaseField):
                attrs[key].name = key
                fields[key] = attrs[key]
                if getattr(attrs[key], 'primary', False):
                    if has_primary_key:
                        raise 'The model must not has two primary key field.'
                    has_primary_key = True
                    primary_key = key
        if not has_primary_key:
            key = 'id'
            attrs[key] = IntegerField(primary=True, verbose_name='ID')
            attrs[key].name = key
            fields['id'] = attrs[key]
            primary_key = key
        attrs['fields'] = fields

        attrs['meta'] = {}
        meta_data = attrs.get('Meta', type(str('Meta'), (), {}))
        attrs['meta']['table_name'] = getattr(meta_data, 'table_name', name)
        attrs['meta']['primary_key'] = primary_key
        return type.__new__(cls, name, bases, attrs)

    def __init__(self, *args, **kwargs):
        self.objects = QuerySet(self)


class Model(object):
    __metaclass__ = MetaModel

    def __init__(self, **kwargs):
        for field, value in kwargs.items():
            if field not in self.fields.keys():
                raise FieldNotExists('Field {} is not exists. You must use these field: {}'
                                     .format(field, ', '.join(self.fields.keys())))
            setattr(self, field, value)

    @classmethod
    def migrate_sql(cls):
        sql = 'CREATE TABLE {} (\n{}\n)'.format(cls.meta['table_name'],
                                      ',\n'.join([field.migrate_sql() \
                                                for field in cls.fields.values()
                                                ]))
        return sql

    def save(self):
        fields = self._get_save_field()
        values = self._get_save_field_value(fields)
        sql = self._get_save_sql(fields, values)
        with connection as conn:
            conn.execute(sql)

    def _get_save_field(self):
        _fields = []
        for field in self.fields:
            if not isinstance(getattr(self, field), BaseField):
                _fields.append(field)
        return _fields

    def _get_save_field_value(self, fields):
        values = []
        for field in fields:
            value = getattr(self, field)
            value = getattr(self.__class__, field).validate(value)
            values.append(getattr(self.__class__, field).value_to_db(value))
        return values

    def _get_save_sql(self, fields, values):
        sql = 'INSERT INTO {table_name} ({fields}) VALUES ({values})'.format(
            table_name=self.meta['table_name'],
            fields = ', '.join(fields),
            values = ', '.join(values),
        )
        return sql


class Migration(object):
    @staticmethod
    def clean():
        connection.clean()
        connection.connect()

    @classmethod
    def migrate(cls, *models):
        with connection as conn:
            for sql in cls.migrate_sql(*models):
                conn.execute(sql)

    @staticmethod
    def migrate_sql(*models):
        for model in models:
            yield model.migrate_sql()


if __name__ == '__main__':
