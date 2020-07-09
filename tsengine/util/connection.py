from taos import connect


def make_ts_connect(host=None, user=None, password=None, database=None, port=6030, config=None):
    return connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        config=config
    )
