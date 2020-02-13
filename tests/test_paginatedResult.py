import TestHelperSuperClass
import object_store_abstraction

class MockIterator(object_store_abstraction.PaginatedResultIteratorBaseClass):
  list = None
  curIdx = None

  def __init__(self, list):
    def filterFN(a, b):
      return True
    object_store_abstraction.PaginatedResultIteratorBaseClass.__init__(self, "", filterFN)
    self.curIdx = 0
    self.list = list

  def _next(self):
    self.curIdx = self.curIdx + 1
    if self.curIdx > len(self.list):
      return None
    return self.list[self.curIdx-1]


#@TestHelperSuperClass.wipd
class test_paginatedResult(TestHelperSuperClass.testHelperSuperClass):

  def test_getPaginatedResultUsingIterator(self):
    mockIterator = MockIterator([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingIterator(
      iteratorObj=mockIterator,
      outputFN=outputFn,
      offset=4,
      pagesize=4
    )
    self.assertEqual(res["pagination"], { "offset": 4, "pagesize": 4, "total": 9})
    self.assertEqual(res["result"], [5, 6, 7, 8])

  def test_getPaginatedResultUsingIteratorALLLIST(self):
    mockIterator = MockIterator([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingIterator(
      iteratorObj=mockIterator,
      outputFN=outputFn,
      offset=0,
      pagesize=42
    )
    self.assertEqual(res["pagination"], { "offset": 0, "pagesize": 42, "total": 12})
    self.assertEqual(res["result"], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

  def test_getPaginatedResultUsingIteratorSTARTLIST(self):
    mockIterator = MockIterator([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingIterator(
      iteratorObj=mockIterator,
      outputFN=outputFn,
      offset=0,
      pagesize=2
    )
    self.assertEqual(res["pagination"], { "offset": 0, "pagesize": 2, "total": 3})
    self.assertEqual(res["result"], [1, 2])

  def test_getPaginatedResultUsingIteratorENDLIST(self):
    mockIterator = MockIterator([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingIterator(
      iteratorObj=mockIterator,
      outputFN=outputFn,
      offset=10,
      pagesize=42
    )
    self.assertEqual(res["pagination"], { "offset": 10, "pagesize": 42, "total": 12})
    self.assertEqual(res["result"], [11, 12])

  def test_getPaginatedResultUsingPythonIterator(self):
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingPythonIterator(
      iteratorObj=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].__iter__(),
      outputFN=outputFn,
      offset=4,
      pagesize=4
    )
    self.assertEqual(res["pagination"], { "offset": 4, "pagesize": 4, "total": 9})
    self.assertEqual(res["result"], [5, 6, 7, 8])

  def test_getPaginatedResultUsingPythonIteratorALLLIST(self):
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingPythonIterator(
      iteratorObj=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].__iter__(),
      outputFN=outputFn,
      offset=0,
      pagesize=42
    )
    self.assertEqual(res["pagination"], { "offset": 0, "pagesize": 42, "total": 12})
    self.assertEqual(res["result"], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

  def test_getPaginatedResultUsingPythonIteratorSTARTLIST(self):
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingPythonIterator(
      iteratorObj=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].__iter__(),
      outputFN=outputFn,
      offset=0,
      pagesize=2
    )
    self.assertEqual(res["pagination"], { "offset": 0, "pagesize": 2, "total": 3})
    self.assertEqual(res["result"], [1, 2])

  def test_getPaginatedResultUsingPythonIteratorENDLIST(self):
    def outputFn(item):
      return item
    res = object_store_abstraction.getPaginatedResultUsingPythonIterator(
      iteratorObj=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].__iter__(),
      outputFN=outputFn,
      offset=10,
      pagesize=42
    )
    self.assertEqual(res["pagination"], { "offset": 10, "pagesize": 42, "total": 12})
    self.assertEqual(res["result"], [11, 12])
