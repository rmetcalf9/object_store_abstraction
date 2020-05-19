import unittest
import TestHelperSuperClass
from object_store_abstraction import UniqueQueue

#@TestHelperSuperClass.wipd
class test_UniqueuQueue_TestClass(unittest.TestCase):

  def test_uniqueQueueMain(self):
    q = UniqueQueue(maxsize=123)

    self.assertEqual(q.qsize(),0)
    q.put("A")
    q.put("B")
    q.put("C")
    self.assertEqual(q.qsize(),3)
    self.assertEqual(q.get(block=False), "A")
    self.assertEqual(q.get(block=False), "B")
    self.assertEqual(q.get(block=False), "C")
    self.assertEqual(q.qsize(),0)
    q.put("A")
    q.put("A")
    q.put("A")
    self.assertEqual(q.qsize(),1)
    self.assertEqual(q.get(block=False), "A")
    self.assertEqual(q.qsize(),0)
