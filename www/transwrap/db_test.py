#!/usr/bin/env python
# encoding: utf-8

import unittest, db

class DbTestCase(unittest.TestCase):

    def setUp(self):
        """TODO: Docstring for setUp.
        :returns: TODO

        """
        pass

    def tearDown(self):
        pass


    def testDict(self):
        """TODO: Docstring for testDict.
        :returns: TODO

        """
        d1 = db.Dict(name='zhangsan',sex='male')
        self.assertEqual(d1.name,'zhangsan')
        self.assertEqual(d1.sex,'female')

if __name__ == '__main__':
    unittest.main()
