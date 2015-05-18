__author__ = 'jonathan'

import unittest
from lib.rome.core.session.session import Session as Session
from lib.rome.core.orm.query import Query as Query
from test.test_dogs import *

class TestMemoization(unittest.TestCase):

    def test_session_execution(self):
        logging.getLogger().setLevel(logging.DEBUG)
        session = Session()
        with session.begin():
            dogs = Query(Dog).all()
            print("dogs: %s" % (dogs))
            print("session_id: %s" % (session.session_id))
            dogs[0].name += "aa"
            dogs[0].save(session=session)
            # raise Exception("toto")

    def test_concurrent_update(self):
        import threading
        import time

        def work():
            logging.getLogger().setLevel(logging.DEBUG)
            session = Session()
            with session.begin():
                dogs = Query(Dog).all()
                time.sleep(1)
                print("dogs: %s" % (dogs))
                print("session_id: %s" % (session.session_id))
                dogs[0].name += "bb"
                dogs[0].save(session=session)

        a = threading.Thread(None, work(), None)
        b = threading.Thread(None, work(), None)
        a.start()
        b.start()


if __name__ == '__main__':
    unittest.main()