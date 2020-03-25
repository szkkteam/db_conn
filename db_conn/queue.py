# Common Python library imports
import multiprocessing as mp
import multiprocessing.queues as mpq
from threading import Thread
import traceback


# Pip package imports
from loguru import logger

# Internal package imports
from db_conn.utils import retry
from db_conn.connection.postgresql import ConnectionPool

class InsertQueue(mpq.Queue):

    def __init__(self, *args, **kwargs):
        size = kwargs.get('size', 120)
        self.num_workers = kwargs.get('max_workers', 1)
        self.pool = kwargs.get('pool', ConnectionPool())
        self.name = kwargs.get('name', "Unknown")
        self.workers = []

        #Queue.__init__(self, size)
        ctx = mp.get_context()
        super(InsertQueue, self).__init__(size, ctx=ctx)
        self._hire_workers()

    def _hire_workers(self):
        logger.info("[%s] Queue handler is hiring \'%s\' worker." % (self.name, self.num_workers))
        for i in range(self.num_workers):
            t = Thread(target=self._worker, args=(i,))
            self.workers.append(t)
            t.daemon = True
            t.start()

    def fire_workers(self):
        if len(self.workers) == 0:
            # Nothing to do, no workers running
            return
        logger.info("[%s] Queue handler is firing \'%s\' worker." % (self.name, self.num_workers))
        result_lst = []
        for i in range(self.num_workers):
            self.put(None)
        for t in self.workers:
            t.join()
            result_lst.append({ 'name': self.name, 'result': True })
        self.workers = []
        return result_lst

    @retry(Exception, delay=10)
    def _worker(self, thread_num):
        try:
            with self.pool.get_connection() as conn:
                while True:
                    d = self.get()
                    if d is None:
                        break

                    with self.pool.get_cursor(connection=conn, commit=True) as cur:
                        cur.execute(str(d))
        except Exception as err:
            tb = traceback.format_exc()
            logger.error("Broken Query: %s" % d)
            logger.error(tb)
            # TODO: Maybe this can fix it?
            self.pool.restart()
            raise
        else:
            logger.info("[%s] Queue handler: \'%s\' exited safely." % (self.name, thread_num))
            return None

