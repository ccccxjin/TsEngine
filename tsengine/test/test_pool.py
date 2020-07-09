import pytest


class TestPool:
    def test_pool_get(self, pool):
        assert pool.checkedin == 1
        assert pool.checkedout == 0
        connection1 = pool.get()
        assert pool.checkedin == 0
        assert pool.checkedout == 1
        connection2 = pool.get()
        assert pool.checkedin == 0
        assert pool.checkedout == 2
        connection3 = pool.get()
        assert pool.checkedin == 0
        assert pool.checkedout == 3
        assert pool.busy

        pool.wait = False
        with pytest.raises(TimeoutError) as e:
            pool.get()
        exec_msg = e.value.args[0]
        assert exec_msg == 'get session timeout error'

        pool.wait = 3
        with pytest.raises(TimeoutError) as e:
            pool.get()
        exec_msg = e.value.args[0]
        assert exec_msg == 'get session timeout error'

        pool.put(connection1)
        assert pool.checkedin == 1
        assert pool.checkedout == 2

        pool.put(connection2)
        assert pool.checkedin == 2
        assert pool.checkedout == 1

        pool.put(connection3)
        assert pool.checkedin == 3
        assert pool.checkedout == 0
