

def create_keyspace(cluster, name,
                    strategy_class='SimpleStrategy',
                    replication_factor=3):
    """Creates a keyspace
       :param cluster: instance of cassandra.Cluster
       :param name: name of keyspace
       :param strategy_class: replication strategy class
       :param replication_factor: replication factor
    """
    session = cluster.connect()
    if name not in cluster.metadata.keyspaces:
        query = """CREATE KEYSPACE %s
                   WITH REPLICATION = {'class': '%s', 'replication_factor': %d}
                """ % (name, strategy_class, replication_factor,)
        session.execute(query)
    session.shutdown()


def drop_keyspace(cluster, name):
    """Drops a keyspace
       :param cluster: instance of cassandra.Cluster
       :param name: name of keyspace
    """
    session = cluster.connect()
    if name in cluster.metadata.keyspaces:
        session.execute("DROP KEYSPACE %s" % name)
    session.shutdown()


def create_table(cluster, keyspace, name, columns, primary_key):
    """Created a table
       :param cluster: instance of cassandra.Cluster
       :param keyspace: keyspace name
       :param name: name of table
       :param columns: dict of columns names and types
       :param primary_key: list of columns included to promary key
    """
    session = cluster.connect()
    if keyspace not in cluster.metadata.keyspaces:
        create_keyspace(cluster, keyspace)
    session.set_keyspace(keyspace)
    query = """CREATE TABLE IF NOT EXISTS %s
               (%s, PRIMARY KEY(%s))
            """ % (name,
                   ', '.join(['%s %s' % (k, v) for k, v in columns.items()]),
                   ', '.join(primary_key),)
    session.execute(query)
    session.shutdown()
