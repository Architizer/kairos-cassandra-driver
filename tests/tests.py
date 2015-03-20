
import unittest
from collections import OrderedDict

from cassandra.cluster import Cluster

from kairos_cassandra_driver import (
    Timeseries,
    CassandraSeries,
)
from kairos_cassandra_driver.utils import (
    create_keyspace,
    drop_keyspace,
)


TEST_KEYSPACE = 'kcd_test_keyspace'


class TestCassandraTimeseries(unittest.TestCase):

    def setUp(self):
        self.intervals = {
            'minute': {
                'step': 60,
                'steps': 5,
            },
            'hour': {
                'step': 3600,
                'resolution': 60,
            }
        }
        self.cluster = Cluster(
            ['127.0.0.1'],
            port=9042
        )
        create_keyspace(self.cluster, TEST_KEYSPACE)

    def tearDown(self):
        drop_keyspace(self.cluster, TEST_KEYSPACE)
        self.cluster.shutdown()

    def _time(self, t):
        return (500000*3600) + t


class TestTimeseries(TestCassandraTimeseries):

    def test_backends_choise(self):
        series = Timeseries(self.cluster,
                            type='series',
                            intervals=self.intervals,
                            keyspace=TEST_KEYSPACE)
        self.assertTrue(isinstance(series, CassandraSeries))


class TestCassandraSeries(TestCassandraTimeseries):

    def setUp(self):
        super(TestCassandraSeries, self).setUp()
        self.series = Timeseries(self.cluster,
                                 type='series',
                                 intervals=self.intervals,
                                 keyspace=TEST_KEYSPACE)

    def test_bulk_insert(self):
        inserts = {
            None: {'test1': [1, 2, 3], 'test2': [4, 5, 6]},
            self._time(0): {'test1': [1, 2, 3], 'test2': [4, 5, 6],
                            'test3': [7, 8, 9]},
            self._time(30): {'test1': [1, 2, 3], 'test2': [4, 5, 6]},
            self._time(60): {'test1': [1, 2, 3], 'test3': [7, 8, 9]}
        }
        self.series.bulk_insert(inserts)

        t1_i1 = self.series.get('test1', 'minute', timestamp=self._time(0))
        self.assertEqual([1, 2, 3, 1, 2, 3], t1_i1[self._time(0)])

        t2_i1 = self.series.get('test2', 'minute', timestamp=self._time(0))
        self.assertEqual([4, 5, 6, 4, 5, 6], t2_i1[self._time(0)])

        t3_i1 = self.series.get('test3', 'minute', timestamp=self._time(0))
        self.assertEqual([7, 8, 9], t3_i1[self._time(0)])

        t1_i2 = self.series.get('test1', 'minute', timestamp=self._time(60))
        self.assertEqual([1, 2, 3], t1_i2[self._time(60)])

    def test_bulk_insert_intervals_after(self):
        a, b, c, d, e, f = 10, 11, 12, 13, 14, 15
        inserts = OrderedDict((
            (None, {'test1': [1, 2, 3], 'test2': [4, 5, 6]}),
            (self._time(0), {'test1': [1, 2, 3],
                             'test2': [4, 5, 6], 'test3': [7, 8, 9]}),
            (self._time(30), {'test1': [1, 2, 3], 'test2': [4, 5, 6]}),
            (self._time(60), {'test1': [a, b, c], 'test3': [d, e, f]})
        ))
        self.series.bulk_insert(inserts, intervals=3)

        t1_i1 = self.series.get('test1', 'minute', timestamp=self._time(0))
        self.assertEqual([1, 2, 3, 1, 2, 3], t1_i1[self._time(0)])

        t2_i1 = self.series.get('test2', 'minute', timestamp=self._time(0))
        self.assertEqual([4, 5, 6, 4, 5, 6], t2_i1[self._time(0)])

        t3_i1 = self.series.get('test3', 'minute', timestamp=self._time(0))
        self.assertEqual([7, 8, 9], t3_i1[self._time(0)])

        t1_i2 = self.series.get('test1', 'minute', timestamp=self._time(60))
        self.assertEqual([1, 2, 3, 1, 2, 3, a, b, c], t1_i2[self._time(60)])

        t3_i3 = self.series.get('test3', 'minute', timestamp=self._time(120))
        self.assertEqual([7, 8, 9, d, e, f], t3_i3[self._time(120)])

        t3_i4 = self.series.get('test3', 'minute', timestamp=self._time(180))
        self.assertEqual([7, 8, 9, d, e, f], t3_i4[self._time(180)])


if __name__ == '__main__':
    unittest.main()
