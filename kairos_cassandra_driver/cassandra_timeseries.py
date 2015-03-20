'''
Copyright (c) 2015, Maria Kuznetsova
Copyright (c) 2012-2014, Agora Games, LLC All rights reserved.
https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''

import kairos
from kairos.timeseries import (BACKENDS, Series, Histogram,
                               Gauge, Set, Count,)
from kairos.cassandra_backend import TYPE_MAP, QUOTE_TYPES, QUOTE_MATCH

from collections import OrderedDict

from .utils import create_table


class Timeseries(kairos.Timeseries):
    """ Base class of all time series.
        Also acts as a factory to return the correct subclass
        if 'type=' keyword argument supplied.
        Uses cassandra-driver for 'cassandra' backend
        ('client' should be a cassandra.cluster.Cluster instance."""

    def __new__(cls, client, **kwargs):
        client_module = client.__module__.split('.')[0]
        backend = BACKENDS.get(client_module)
        if backend:
            return backend(client, **kwargs)

        raise ImportError(
            "Unsupported or unknown client type %s", client_module)


class CassandraBackend(Timeseries):

    cluster = None
    session = None
    default_columns = {
        'name': 'text',
        'interval': 'text',
        'i_time': 'bigint',
        'r_time': 'bigint'}

    def __new__(cls, *args, **kwargs):
        ttypes_map = {
            'series': CassandraSeries,
            'histogram': CassandraHistogram,
            'count': CassandraCount,
            'gauge': CassandraGauge,
            'set': CassandraSet,
        }
        ttype = kwargs.get('type', None)
        type_cls = ttypes_map.get(ttype)
        if type_cls:
            return type_cls.__new__(type_cls, *args, **kwargs)
        raise NotImplementedError("No implementation for %s type" % ttype)

    def __init__(self, client, **kwargs):
        value_type = kwargs.get('value_type', float)
        self._value_type = TYPE_MAP[value_type]
        self._table = kwargs.get('table_name', self._table)
        self.cluster = client
        self._keyspace = kwargs.get('keyspace', 'kairos')
        super(CassandraBackend, self).__init__(client, **kwargs)

    def _get_session(self):
        if self.session is not None:
            return self.session
        self.session = self.cluster.connect(self._keyspace)
        return self.session

    def _shutdown_session(self):
        if self.session:
            self.session.shutdown()
            self.session = None

    def _insert(self, name, value, timestamp, intervals, **kwargs):
        if self._value_type in QUOTE_TYPES and not QUOTE_MATCH.match(value):
            value = "'%s'" % (value)
        for interval, config in self._intervals.items():
            timestamps = self._normalize_timestamps(
                timestamp, intervals, config)
            for tstamp in timestamps:
                self._insert_data(
                    name, value, tstamp, interval, config, **kwargs)

    def _insert_data(self, name, value, timestamp, interval, config):
        stmt = self._insert_stmt(name, value, timestamp, interval, config)
        if stmt:
            self._get_session().execute(stmt)

    def _insert_stmt(self, name, value, timestamp, interval, config):
        raise NotImplementedError

    def _get(self, name, interval, config, timestamp, **kwargs):
        i_bucket = config['i_calc'].to_bucket(timestamp)
        fetch = kwargs.get('fetch')
        process_row = kwargs.get('process_row') or self._process_row

        rval = OrderedDict()
        if fetch:
            data = fetch(self._get_session(),
                         self._table, name, interval, [i_bucket])
        else:
            data = self._type_get(name, interval, i_bucket)

        if config['coarse']:
            rval[config['i_calc'].from_bucket(i_bucket)] = (
                process_row(data.values()[0][None])
                if data
                else self._type_no_value()
            )
        else:
            for r_bucket, row_data in data.values()[0].items():
                rval[config['r_calc'].from_bucket(r_bucket)] = process_row(row_data)

        return rval

    def _series(self, name, interval, config, buckets, **kws):
        fetch = kws.get('fetch')
        process_row = kws.get('process_row') or self._process_row

        rval = OrderedDict()

        if fetch:
            data = fetch(self._get_session(),
                         self._table, name, interval, buckets)
        else:
            data = self._type_get(name, interval, buckets[0], buckets[-1])

        if config['coarse']:
            for i_bucket in buckets:
                i_key = config['i_calc'].from_bucket(i_bucket)
                i_data = data.get(i_bucket)
                if i_data:
                    rval[i_key] = process_row(i_data[None])
                else:
                    rval[i_key] = self._type_no_value()
        elif data:
            for i_bucket, i_data in data.items():
                i_key = config['i_calc'].from_bucket(i_bucket)
                rval[i_key] = OrderedDict()
                for r_bucket, r_data in i_data.items():
                    r_key = config['r_calc'].from_bucket(r_bucket)
                    if r_data:
                        rval[i_key][r_key] = process_row(r_data)
                    else:
                        rval[i_key][r_key] = self._type_no_value()

        return rval

    def delete(self, name):
        self._get_session().execute(
            "DELETE FROM %s WHERE name='%s'" % (self._table, name))

    def delete_all(self):
        self._get_session().execute("TRUNCATE %s", [self._table])

    def list(self):
        res = self._get_session().execute(
            "SELECT name FROM %s", [self._table])
        return [row.name for row in res]

    def properties(self, name):
        rval = {}

        for interval, config in self._intervals.items():
            rval.setdefault(interval, {})

            i_time_first = self._get_session().execute(
                '''SELECT i_time
                   FROM %s
                   WHERE name=%s AND interval=%s
                   ORDER BY interval ASC, i_time ASC
                   LIMIT 1''',
                [self._table, name, interval]
            )
            rval[interval]['first'] = config['i_calc'].from_bucket(
                i_time_first[0].i_time)
            i_time_last = self._get_session().execute(
                '''SELECT i_time
                   FROM %s
                   WHERE name=%s AND interval=%s
                   ORDER BY interval ASC, i_time ASC
                   LIMIT 1''',
                [self._table, name, interval]
            )
            rval[interval]['last'] = config['i_calc'].from_bucket(
                i_time_last[0].i_time)

        return rval


class CassandraSeries(CassandraBackend, Series):

    def __new__(cls, *args, **kwargs):
        return Series.__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        self._table = 'series'
        super(CassandraSeries, self).__init__(*args, **kwargs)
        self.default_columns.update(
            {'value': ('list<%s>' % self._value_type)})

        create_table(self.cluster, self._keyspace, self._table,
                     self.default_columns,
                     ['name', 'interval', 'i_time', 'r_time'])

    def _insert_stmt(self, name, value, timestamp, interval, config):
        '''Helper to generate the insert statement.'''
        # Calculate the TTL and abort if inserting into the past
        expire, ttl = config['expire'], config['ttl'](timestamp)
        if expire and not ttl:
            return None

        i_time = config['i_calc'].to_bucket(timestamp)
        if not config['coarse']:
            r_time = config['r_calc'].to_bucket(timestamp)
        else:
            r_time = -1

        table_spec = self._table
        if ttl:
            table_spec += " USING TTL %s " % (ttl)
        stmt = '''UPDATE %s SET value = value + [%s]
                  WHERE name = '%s'
                  AND interval = '%s'
                  AND i_time = %s
                  AND r_time = %s''' % (table_spec, value, name,
                                        interval, i_time, r_time)
        return stmt

    def _type_get(self, name, interval, i_bucket, i_end=None):
        rval = OrderedDict()

        query = """SELECT i_time, r_time, value
                   FROM %(table)s
                   WHERE name = '%(name)s'
                   AND interval = '%(interval)s'"""
        extra_query = 'AND i_time >= %(i_bucket)s AND i_time <= %(i_end)s' if i_end else 'AND i_time = %(i_bucket)s'
        order_query = 'ORDER BY interval, i_time, r_time'
        query = ' '.join([query, extra_query, order_query])

        stmt = query % {'name': name, 'table': self._table,
                        'interval': interval, 'i_bucket': i_bucket,
                        'i_end': i_end}
        rows = self._get_session().execute(stmt)
        for row in rows:
            r_time = None if row.r_time == -1 else row.r_time
            rval.setdefault(row.i_time, OrderedDict())[r_time] = row.value

        return rval


class CassandraHistogram(CassandraBackend, Histogram):

    def __init__(self, *args, **kwargs):
        self._table = 'histogram'
        super(CassandraHistogram, self).__init__(*args, **kwargs)

        self.default_columns.update(
            {'value': self._value_type, 'count': 'counter'})
        create_table(self.cluster, self._keyspace, self._table,
                     self.default_columns,
                     ['name', 'interval', 'i_time', 'r_time', 'value'])

    def _insert_stmt(self, name, value, timestamp, interval, config):
        '''Helper to generate the insert statement.'''
        # Calculate the TTL and abort if inserting into the past
        expire, ttl = config['expire'], config['ttl'](timestamp)
        if expire and not ttl:
            return None

        i_time = config['i_calc'].to_bucket(timestamp)
        if not config['coarse']:
            r_time = config['r_calc'].to_bucket(timestamp)
        else:
            r_time = -1

        table_spec = self._table
        if ttl:
            table_spec += " USING TTL %s " % (ttl)
        stmt = """UPDATE %s SET count = count + 1
                  WHERE name = '%s'
                  AND interval = '%s'
                  AND i_time = %s
                  AND r_time = %s
                  AND value = %s""" % (table_spec, name,
                                       interval, i_time,
                                       r_time, value)
        return stmt

    def _type_get(self, name, interval, i_bucket, i_end=None):
        rval = OrderedDict()

        stmt = """SELECT i_time, r_time, value, count
                  FROM %s
                  WHERE name = '%s' AND interval = '%s'
               """ % (self._table, name, interval)
        if i_end:
            stmt += ' AND i_time >= %s AND i_time <= %s' % (i_bucket, i_end)
        else:
            stmt += ' AND i_time = %s' % (i_bucket)
        stmt += ' ORDER BY interval, i_time, r_time'

        rows = self._get_session().execute(stmt)
        for row in rows:
            r_time = None if row.r_time == -1 else row.r_time
            rval.setdefault(row.i_time, OrderedDict()).setdefault(
                r_time, {})[row.value] = row.count
        return rval


class CassandraCount(CassandraBackend, Count):

    def __init__(self, *args, **kwargs):
        self._table = 'count'
        super(CassandraCount, self).__init__(*args, **kwargs)

        self.default_columns.update({'count': 'counter'})
        create_table(self.cluster, self._keyspace, self._table,
                     self.default_columns,
                     ['name', 'interval', 'i_time', 'r_time'])

    def _insert_stmt(self, name, value, timestamp, interval, config):
        '''Helper to generate the insert statement.'''
        # Calculate the TTL and abort if inserting into the past
        expire, ttl = config['expire'], config['ttl'](timestamp)
        if expire and not ttl:
            return None

        i_time = config['i_calc'].to_bucket(timestamp)
        r_time = (-1 if config['coarse']
                  else config['r_calc'].to_bucket(timestamp))

        table_spec = self._table
        if ttl:
            table_spec += " USING TTL %s " % (ttl)
        stmt = '''UPDATE %s SET count = count + %s
                  WHERE name = '%s'
                  AND interval = '%s'
                  AND i_time = %s
                  AND r_time = %s''' % (table_spec, value,
                                        name, interval,
                                        i_time, r_time)
        return stmt

    def _type_get(self, name, interval, i_bucket, i_end=None):
        rval = OrderedDict()

        stmt = """SELECT i_time, r_time, count
                  FROM %s
                  WHERE name = '%s' AND interval = '%s'
               """ % (self._table, name, interval)
        if i_end:
            stmt += ' AND i_time >= %s AND i_time <= %s'%(i_bucket, i_end)
        else:
            stmt += ' AND i_time = %s'%(i_bucket)
        stmt += ' ORDER BY interval, i_time, r_time'

        rows = self._get_session().execute(stmt)
        for row in rows:
            r_time = None if row.r_time == -1 else row.r_time
            rval.setdefault(row.i_time, OrderedDict())[r_time] = row.count
        return rval


class CassandraGauge(CassandraBackend, Gauge):

    def __init__(self, *args, **kwargs):
        self._table = 'gauge'
        super(CassandraGauge, self).__init__(*args, **kwargs)

        self.default_columns.update({'value': self._value_type})
        create_table(self.cluster, self._keyspace, self._table,
                     self.default_columns,
                     ['name', 'interval', 'i_time', 'r_time'])

    def _insert_stmt(self, name, value, timestamp, interval, config):
        '''Helper to generate the insert statement.'''
        # Calculate the TTL and abort if inserting into the past
        expire, ttl = config['expire'], config['ttl'](timestamp)
        if expire and not ttl:
            return None

        i_time = config['i_calc'].to_bucket(timestamp)
        if not config['coarse']:
            r_time = config['r_calc'].to_bucket(timestamp)
        else:
            r_time = -1

        table_spec = self._table
        if ttl:
            table_spec += " USING TTL %s " % ttl
        stmt = """UPDATE %s SET value = %s
                  WHERE name = '%s'
                  AND interval = '%s'
                  AND i_time = %s
                  AND r_time = %s""" % (table_spec, value,
                                        name, interval,
                                        i_time, r_time)
        return stmt

    def _type_get(self, name, interval, i_bucket, i_end=None):
        rval = OrderedDict()

        stmt = """SELECT i_time, r_time, value
                  FROM %s
                  WHERE name = '%s' AND interval = '%s'
               """ % (self._table, name, interval)
        if i_end:
            stmt += ' AND i_time >= %s AND i_time <= %s' % (i_bucket, i_end)
        else:
            stmt += ' AND i_time = %s'%(i_bucket)
        stmt += ' ORDER BY interval, i_time, r_time'

        rows = self._get_session().execute(stmt)
        for row in rows:
            r_time = None if row.r_time == -1 else row.r_time
            rval.setdefault(row.i_time, OrderedDict())[r_time] = row.value
        return rval


class CassandraSet(CassandraBackend, Set):

    def __init__(self, *args, **kwargs):
        self._table = 'sets'
        super(CassandraSet, self).__init__(*args, **kwargs)

        self.default_columns.update({'value': self._value_type})
        create_table(self.cluster, self._keyspace, self._table,
                     self.default_columns,
                     ['name', 'interval', 'i_time', 'r_time', 'value'])

    def _insert_stmt(self, name, value, timestamp, interval, config):
        '''Helper to generate the insert statement.'''
        # Calculate the TTL and abort if inserting into the past
        expire, ttl = config['expire'], config['ttl'](timestamp)
        if expire and not ttl:
            return None

        i_time = config['i_calc'].to_bucket(timestamp)
        if not config['coarse']:
            r_time = config['r_calc'].to_bucket(timestamp)
        else:
            r_time = -1

        stmt = """INSERT INTO %s (name, interval, i_time, r_time, value)
                  VALUES ('%s', '%s', %s, %s, %s)
               """ % (self._table, name, interval, i_time, r_time, value)
        expire = config['expire']
        if ttl:
            stmt += " USING TTL %s" % ttl
        return stmt

    def _type_get(self, name, interval, i_bucket, i_end=None):
        rval = OrderedDict()

        stmt = """SELECT i_time, r_time, value
                  FROM %s
                  WHERE name = '%s' AND interval = '%s'
               """ % (self._table, name, interval)
        if i_end:
            stmt += ' AND i_time >= %s AND i_time <= %s' % (i_bucket, i_end)
        else:
            stmt += ' AND i_time = %s' % (i_bucket)
        stmt += ' ORDER BY interval, i_time, r_time'

        rows = self._get_session().execute(stmt)
        for row in rows:
            r_time = None if row.r_time == -1 else row.r_time
            rval.setdefault(row.i_time, OrderedDict()).setdefault(
                r_time, set()).add(row.value)
        return rval


BACKENDS.update({'cassandra': CassandraBackend})
