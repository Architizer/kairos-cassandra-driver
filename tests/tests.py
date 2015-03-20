
import time
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
        return (500000 * 3600) + t


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
            (self._time(0), {'test1': [1, 2, 3], 'test2': [4, 5, 6],
                             'test3': [7, 8, 9]}),
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

    def test_bulk_insert_intervals_before(self):
        a, b, c, d, e, f = 10, 11, 12, 13, 14, 15
        inserts = OrderedDict((
            (None, {'test1': [1, 2, 3], 'test2': [4, 5, 6]}),
            (self._time(0), {'test1': [1, 2, 3], 'test2': [4, 5, 6], 'test3': [7, 8, 9]}),
            (self._time(30), {'test1': [1, 2, 3], 'test2': [4, 5, 6]}),
            (self._time(60), {'test1': [a, b, c], 'test3': [d, e, f]})
        ))
        self.series.bulk_insert(inserts, intervals=-3)

        t1_i1 = self.series.get('test1', 'minute', timestamp=self._time(0))
        self.assertEqual([1, 2, 3, 1, 2, 3, a, b, c], t1_i1[self._time(0)])

        t2_i1 = self.series.get('test2', 'minute', timestamp=self._time(0))
        self.assertEqual([4, 5, 6, 4, 5, 6], t2_i1[self._time(0)])
    
        t3_i1 = self.series.get('test3', 'minute', timestamp=self._time(0))
        self.assertEqual([7, 8, 9, d, e, f], t3_i1[self._time(0)])

        t1_i2 = self.series.get('test1', 'minute', timestamp=self._time(-60))
        self.assertEqual([1, 2, 3, 1, 2, 3, a, b, c], t1_i2[self._time(-60)])

        t3_i3 = self.series.get('test3', 'minute', timestamp=self._time(-120))
        self.assertEqual([7, 8, 9, d, e, f], t3_i3[self._time(-120)])

        t3_i4 = self.series.get('test3', 'minute', timestamp=self._time(-180))
        self.assertEqual([7, 8, 9], t3_i4[self._time(-180)])

    def test_insert_multiple_intervals_after(self):
        ts1 = self._time(0)
        ts2 = self.series._intervals['minute']['i_calc'].normalize(ts1, 1)
        ts3 = self.series._intervals['minute']['i_calc'].normalize(ts1, 2)
        self.assertNotEqual(ts1, ts2)

        self.series.insert('test', 32, timestamp=ts1, intervals=1)

        interval_1 = self.series.get('test', 'minute', timestamp=ts1)
        self.assertEqual([32], interval_1[ts1])

        interval_2 = self.series.get('test', 'minute', timestamp=ts2)
        self.assertEqual([32], interval_2[ts2])

        self.series.insert('test', 42, timestamp=ts1, intervals=2)

        interval_1 = self.series.get('test', 'minute', timestamp=ts1)
        self.assertEqual([32, 42], interval_1[ts1])
        interval_2 = self.series.get('test', 'minute', timestamp=ts2)
        self.assertEqual([32, 42], interval_2[ts2])
        interval_3 = self.series.get('test', 'minute', timestamp=ts3)
        self.assertEqual([42], interval_3[ts3])

    def test_insert_multiple_intervals_before(self):
        ts1 = self._time(0)
        ts2 = self.series._intervals['minute']['i_calc'].normalize(ts1, -1)
        ts3 = self.series._intervals['minute']['i_calc'].normalize(ts1, -2)
        self.assertNotEqual(ts1, ts2)

        self.series.insert('test', 32, timestamp=ts1, intervals=-1)

        interval_1 = self.series.get('test', 'minute', timestamp=ts1)
        self.assertEqual([32], interval_1[ts1])

        interval_2 = self.series.get('test', 'minute', timestamp=ts2)
        self.assertEqual([32], interval_2[ts2])

        self.series.insert('test', 42, timestamp=ts1, intervals=-2)

        interval_1 = self.series.get('test', 'minute', timestamp=ts1)
        self.assertEqual([32, 42], interval_1[ts1])
        interval_2 = self.series.get('test', 'minute', timestamp=ts2)
        self.assertEqual([32, 42], interval_2[ts2])
        interval_3 = self.series.get('test', 'minute', timestamp=ts3)
        self.assertEqual([42], interval_3[ts3])

    def test_get(self):
        # 2 hours worth of data, value is same asV timestamp
        for t in xrange(1, 7200):
            self.series.insert('test', t, timestamp=self._time(t))

        ###
        ### no resolution, condensed has no impact
        ###
        # middle of an interval
        interval = self.series.get('test', 'minute', timestamp=self._time(100))
        self.assertEqual([self._time(60)], interval.keys())
        self.assertEqual(list(range(60,120)), interval[self._time(60)])

        # end of an interval
        interval = self.series.get('test', 'minute', timestamp=self._time(59))
        self.assertEqual([self._time(0)], interval.keys())
        self.assertEqual(list(range(1, 60)), interval[self._time(0)])

        # no matching interval, returns no with empty value list
        interval = self.series.get('test', 'minute')
        self.assertEqual(1, len(interval))
        self.assertEqual(0, len(interval.values()[0]))

        # with transforms
        interval = self.series.get('test', 'minute',
                                   timestamp=self._time(100),
                                   transform='count')
        self.assertEqual(60, interval[self._time(60)])

        interval = self.series.get('test', 'minute',
                                   timestamp=self._time(100),
                                   transform=['min', 'max'])
        self.assertEqual({'min': 60, 'max': 119}, interval[self._time(60)])

        ###
        ### with resolution, optionally condensed
        ###
        interval = self.series.get('test', 'hour', timestamp=self._time(100))
        self.assertEqual(60, len(interval))
        self.assertEqual(list(range(60, 120)), interval[self._time(60)])
    
        interval = self.series.get('test', 'hour',
                                   timestamp=self._time(100),
                                   condensed=True)
        self.assertEqual(1, len(interval))
        self.assertEqual(list(range(1, 3600)), interval[self._time(0)])

        # with transforms
        interval = self.series.get('test', 'hour',
                                   timestamp=self._time(100),
                                   transform='count')
        self.assertEqual(60, interval[self._time(60)])
    
        interval = self.series.get('test', 'hour',
                                   timestamp=self._time(100),
                                   transform=['min', 'max'], condensed=True)
        self.assertEqual({'min': 1, 'max': 3599}, interval[self._time(0)])

    def test_get_joined(self):
        # put some data in the first minutes of each hour for test1, and then for
        # a few more minutes in test2
        self.series.delete('test1')
        self.series.delete('test2')
        for t in xrange(1, 120):
            self.series.insert('test1', t, timestamp=self._time(t))
            self.series.insert('test2', t, timestamp=self._time(t))
        for t in xrange(3600, 3720):
            self.series.insert('test1', t, timestamp=self._time(t))
            self.series.insert('test2', t, timestamp=self._time(t))
        for t in xrange(120, 240):
            self.series.insert('test1', t, timestamp=self._time(t))
        for t in xrange(3721, 3840):
            self.series.insert('test1', t, timestamp=self._time(t))

        ###
        ### no resolution, condensed has no impact
        ###
        # interval with 2 series worth of data
        interval = self.series.get(['test1', 'test2'], 'minute',
                                   timestamp=self._time(100))
        self.assertEqual([self._time(60)], interval.keys())
        self.assertEqual(list(range(60, 120))+list(range(60, 120)),
                         interval[self._time(60)])

        # interval with 1 series worth of data
        interval = self.series.get(['test1', 'test2'], 'minute',
                                   timestamp=self._time(122))
        self.assertEqual([self._time(120)], interval.keys())
        self.assertEqual(list(range(120, 180)), interval[self._time(120)])

        # no matching interval, returns no with empty value list
        interval = self.series.get(['test1', 'test2'], 'minute')
        self.assertEqual(1, len(interval))
        self.assertEqual(0, len(interval.values()[0]))

        ###
        ### with resolution, optionally condensed
        ###
        interval = self.series.get(['test1', 'test2'], 'hour',
                                   timestamp=self._time(100))
        self.assertEqual(map(self._time, [0, 60, 120, 180]), interval.keys())
        self.assertEqual(list(range(1, 60))+list(range(1, 60)),
                         interval[self._time(0)])
        self.assertEqual(list(range(60, 120))+list(range(60, 120)),
                         interval[self._time(60)])
        self.assertEqual(list(range(120, 180)), interval[self._time(120)])
        self.assertEqual(list(range(180, 240)), interval[self._time(180)])

        interval = self.series.get(['test1', 'test2'], 'hour',
                                   timestamp=self._time(100),
                                   condensed=True)
        self.assertEqual([self._time(0)], interval.keys())
        self.assertEqual(
            list(range(1, 60))+list(range(1, 60))+list(range(60, 120))+list(range(60, 120)) + \
            list(range(120, 180))+list(range(180, 240)),
            interval[self._time(0)])

        # with transforms
        interval = self.series.get(['test1', 'test2'], 'hour',
                                   timestamp=self._time(100),
                                   transform='count')
        self.assertEqual(120, interval[self._time(60)])

        interval = self.series.get(['test1', 'test2'], 'hour',
                                   timestamp=self._time(100),
                                   transform=['min', 'max', 'count'],
                                   condensed=True)
        self.assertEqual({'min': 1, 'max': 239, 'count': 358},
                         interval[self._time(0)])

    def test_series(self):
        # 2 hours worth of data, value is same asV timestamp
        for t in xrange(1, 7200):
            self.series.insert('test', t, timestamp=self._time(t))

        ###
        ### no resolution, condensed has no impact
        ###
        interval = self.series.series('test', 'minute', end=self._time(250))
        self.assertEqual(map(self._time, [0, 60, 120, 180, 240]),
                         interval.keys())
        self.assertEqual(list(range(1, 60)), interval[self._time(0)])
        self.assertEqual(list(range(240, 300)), interval[self._time(240)])
    
        interval = self.series.series('test', 'minute', steps=2,
                                      end=self._time(250))
        self.assertEqual(map(self._time, [180, 240]), interval.keys())
        self.assertEqual(list(range(240, 300)), interval[self._time(240)])

        # with transforms
        interval = self.series.series('test', 'minute', end=self._time(250),
                                      transform=['min', 'count'])
        self.assertEqual(map(self._time, [0, 60, 120, 180, 240]),
                         interval.keys())
        self.assertEqual({'min': 1, 'count': 59}, interval[self._time(0)])
        self.assertEqual({'min': 240, 'count': 60}, interval[self._time(240)])

        # with collapsed
        interval = self.series.series('test', 'minute',
                                      end=self._time(250),
                                      collapse=True)
        self.assertEqual(map(self._time, [0]), interval.keys())
        self.assertEqual(list(range(1, 300)), interval[self._time(0)])

        # with transforms and collapsed
        interval = self.series.series('test', 'minute',
                                      end=self._time(250),
                                      transform=['min', 'count'],
                                      collapse=True)
        self.assertEqual(map(self._time, [0]), interval.keys())
        self.assertEqual({'min': 1, 'count': 299}, interval[self._time(0)])

        ###
        ### with resolution
        ###
        interval = self.series.series('test', 'hour', end=self._time(250))
        self.assertEqual(1, len(interval))
        self.assertEqual(60, len(interval[self._time(0)]))
        self.assertEqual(list(range(1, 60)),
                         interval[self._time(0)][self._time(0)])

        interval = self.series.series('test', 'hour',
                                      end=self._time(250),
                                      transform=['count', 'max'])
        self.assertEqual(1, len(interval))
        self.assertEqual(60, len(interval[self._time(0)]))
        self.assertEqual({'max': 59, 'count': 59},
                         interval[self._time(0)][self._time(0)])

        # single step, last one
        interval = self.series.series('test', 'hour',
                                      condensed=True,
                                      end=self._time(4200))
        self.assertEqual(1, len(interval))
        self.assertEqual(3600, len(interval[self._time(3600)]))
        self.assertEqual(list(range(3600, 7200)), interval[self._time(3600)])

        interval = self.series.series('test', 'hour',
                                      condensed=True,
                                      end=self._time(4200), steps=2)
        self.assertEqual(map(self._time, [0, 3600]), interval.keys())
        self.assertEqual(3599, len(interval[self._time(0)]))
        self.assertEqual(3600, len(interval[self._time(3600)]))
        self.assertEqual(list(range(3600, 7200)), interval[self._time(3600)])
    
        # with transforms
        interval = self.series.series('test', 'hour',
                                      condensed=True,
                                      end=self._time(4200),
                                      transform=['min', 'max'])
        self.assertEqual(1, len(interval))
        self.assertEqual({'min': 3600, 'max': 7199}, interval[self._time(3600)])

        # with collapsed
        interval = self.series.series('test', 'hour',
                                      condensed=True,
                                      end=self._time(4200),
                                      steps=2, collapse=True)
        self.assertEqual(map(self._time, [0]), interval.keys())
        self.assertEqual(7199, len(interval[self._time(0)]))
        self.assertEqual(list(range(1,7200)), interval[self._time(0)])

        # with transforms and collapsed
        interval = self.series.series('test', 'hour', condensed=True,
                                      end=self._time(4200), steps=2,
                                      collapse=True,
                                      transform=['min', 'count', 'max'])
        self.assertEqual(map(self._time, [0]), interval.keys())
        self.assertEqual({'min': 1, 'max': 7199, 'count': 7199},
                         interval[self._time(0)])

    def test_series_joined(self):
        # put some data in the first minutes of each hour for test1, and then for
        # a few more minutes in test2
        self.series.delete('test1')
        self.series.delete('test2')
        for t in xrange(1, 120):
            self.series.insert('test1', t, timestamp=self._time(t))
            self.series.insert('test2', t, timestamp=self._time(t))
        for t in xrange(3600, 3720):
            self.series.insert('test1', t, timestamp=self._time(t))
            self.series.insert('test2', t, timestamp=self._time(t))
        for t in xrange(120, 240):
            self.series.insert('test1', t, timestamp=self._time(t))
        for t in xrange(3720, 3840):
            self.series.insert('test1', t, timestamp=self._time(t))

        ###
        ### no resolution, condensed has no impact
        ###
        interval = self.series.series(['test1', 'test2'], 'minute',
                                      end=self._time(250))
        self.assertEqual(map(self._time, [0, 60, 120, 180, 240]),
                         interval.keys())
        self.assertEqual(list(range(1, 60))+list(range(1, 60)),
                         interval[self._time(0)])
        self.assertEqual(list(range(60, 120))+list(range(60, 120)),
                         interval[self._time(60)])
        self.assertEqual(list(range(120, 180)), interval[self._time(120)])
        self.assertEqual(list(range(180, 240)), interval[self._time(180)])
        self.assertEqual([], interval[self._time(240)])

        # no matching interval, returns no with empty value list
        interval = self.series.series(['test1', 'test2'], 'minute', start=time.time(), steps=2)
        self.assertEqual(2, len(interval))
        self.assertEqual([], interval.values()[0])

        # with transforms
        interval = self.series.series(['test1', 'test2'], 'minute',
                                      end=self._time(250),
                                      transform=['min', 'count'])
        self.assertEqual(map(self._time, [0, 60, 120, 180, 240]),
                         interval.keys())
        self.assertEqual({'min': 1, 'count': 118}, interval[self._time(0)])
        self.assertEqual({'min': 60, 'count': 120}, interval[self._time(60)])
        self.assertEqual({'min': 120, 'count': 60}, interval[self._time(120)])
        self.assertEqual({'min': 180, 'count': 60}, interval[self._time(180)])
        self.assertEqual({'min': 0, 'count': 0}, interval[self._time(240)])

        # with collapsed
        interval = self.series.series(['test1', 'test2'], 'minute',
                                      end=self._time(250), collapse=True)
        self.assertEqual([self._time(0)], interval.keys())
        self.assertEqual(
            list(range(1, 60))+list(range(1, 60))+list(range(60, 120))+list(range(60, 120)) + \
            list(range(120, 180))+list(range(180, 240)),
            interval[self._time(0)])

        # with tranforms and collapsed
        interval = self.series.series(['test1', 'test2'], 'minute',
                                      end=self._time(250),
                                      transform=['min', 'max', 'count'],
                                      collapse=True)
        self.assertEqual([self._time(0)], interval.keys())
        self.assertEqual({'min': 1, 'max': 239, 'count': 358},
                         interval[self._time(0)])

        ###
        ### with resolution, optionally condensed
        ###
        interval = self.series.series(['test1', 'test2'], 'hour',
                                      end=self._time(250))
        self.assertEqual(1, len(interval))
        self.assertEqual(map(self._time, [0, 60, 120, 180]),
                         interval[self._time(0)].keys())
        self.assertEqual(4, len(interval[self._time(0)]))
        self.assertEqual(list(range(1, 60))+list(range(1, 60)),
                         interval[self._time(0)][self._time(0)])
        self.assertEqual(list(range(60, 120))+list(range(60, 120)),
                         interval[self._time(0)][self._time(60)])
        self.assertEqual(list(range(120, 180)),
                         interval[self._time(0)][self._time(120)])
        self.assertEqual(list(range(180, 240)),
                         interval[self._time(0)][self._time(180)])

        # condensed
        interval = self.series.series(['test1', 'test2'], 'hour',
                                      end=self._time(250), condensed=True)
        self.assertEqual([self._time(0)], interval.keys())
        self.assertEqual(
            list(range(1, 60))+list(range(1, 60))+list(range(60, 120))+list(range(60, 120))+\
            list(range(120, 180))+list(range(180, 240)),
            interval[self._time(0)])

        # with collapsed
        interval = self.series.series(['test1', 'test2'], 'hour',
                                      condensed=True,
                                      end=self._time(4200),
                                      steps=2, collapse=True)
        self.assertEqual(map(self._time, [0]), interval.keys())
        self.assertEqual(
            list(range(1, 60))+list(range(1, 60))+list(range(60, 120))+list(range(60, 120)) + \
            list(range(120, 180))+list(range(180, 240)) + \
            list(range(3600, 3660))+list(range(3600, 3660))+list(range(3660, 3720))+list(range(3660, 3720)) + \
            list(range(3720, 3780))+list(range(3780, 3840)),
            interval[self._time(0)])

        # with transforms collapsed
        interval = self.series.series(['test1', 'test2'], 'hour',
                                      condensed=True,
                                      end=self._time(4200),
                                      steps=2, collapse=True,
                                      transform=['min', 'max', 'count'])
        self.assertEqual(map(self._time, [0]), interval.keys())
        self.assertEqual({'min': 1, 'max': 3839, 'count': 718},
                         interval[self._time(0)])
