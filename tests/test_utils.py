import unittest
import pynoddgcs.publish
import utils
import pynoddgcs

class TestSomething(unittest.TestCase):

    def test_something(self):
        self.assertEqual(1,1)

def load_tests(loader, tests, ignore):
    modules = [
        pynoddgcs.publish,
        pynoddgcs.connect
    ]
    for module in modules:
        tests = utils.doctests(module, tests)
    return tests

if __name__ == '__main__':
    unittest.main()
