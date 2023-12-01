import unittest
from s3replace.replace_s3_objects import chunk, filter_dts, filter_keys, parse_dts
from s3replace.replace_s3_objects import split_process_objects, trim_separator


# Unit tests for the helper functions.

class TrimSeparatorTest(unittest.TestCase):
    def test_trim(self):
        self.assertEqual(
            trim_separator('a/b'),
            'a/b')
        self.assertEqual(
            trim_separator('/a/b'),
            'a/b')
        self.assertEqual(
            trim_separator('a/b/'),
            'a/b')
        self.assertEqual(
            trim_separator('/a/b/'),
            'a/b')


class SplitProcessObjectTest(unittest.TestCase):
    def test_split(self):
        self.assertEqual(
            split_process_objects(
                [
                    'a/',
                    'b/',
                    'c/special-cleared-destination',
                    'd/special-full-copy-done',
                ]),
            (
                ['a/', 'b/'],
                ['c/special-cleared-destination'],
                ['d/special-full-copy-done'],
            ))

    def test_split_none(self):
        self.assertEqual(
            split_process_objects(
                [
                    'a/',
                ]),
            (
                ['a/'],
                [],
                [],
            ))


class ParseDtsTest(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(
            parse_dts([
                '/a/dt=2023-01-01/',
                '/a/dt=2023-01-01/',
                '/a/dt=2023-01-02/',
            ]),
            set(['2023-01-01', '2023-01-02']))


class FilterDtsTest(unittest.TestCase):
    def test_all_included(self):
        self.assertEqual(
            filter_dts(
                set(['2023-01-01', '2023-01-02']),
                '2023-01-01',
                '2023-01-03',
            ),
            set(['2023-01-01', '2023-01-02']))

    def test_all_excluded(self):
        self.assertEqual(
            filter_dts(
                set(['2023-01-01', '2023-01-02']),
                '2023-01-05',
                '2023-01-07',
            ),
            set([]))

    def test_some_under(self):
        self.assertEqual(
            filter_dts(
                set(['2023-01-01', '2023-01-02']),
                '2023-01-02',
                '2023-01-03',
            ),
            set(['2023-01-02']))

    def test_some_above(self):
        self.assertEqual(
            filter_dts(
                set(['2023-01-01', '2023-01-02']),
                '2022-12-31',
                '2023-01-02',
            ),
            set(['2023-01-01']))


class FilterKeysTest(unittest.TestCase):
    def test_all_included(self):
        self.assertEqual(
            filter_keys(
                [
                    'a/dt=2023-01-01',
                    'a/dt=2023-01-02',
                    'b/dt=2023-01-01',
                    'b/dt=2023-01-03',
                ],
                '2023-01-01'
            ),
            [
                'a/dt=2023-01-01',
                'b/dt=2023-01-01',
            ])

    def test_none(self):
        self.assertEqual(
            filter_keys(
                [
                    'a/dt=2023-01-01',
                    'a/dt=2023-01-02',
                    'b/dt=2023-01-01',
                    'b/dt=2023-01-03',
                ],
                '2023-01-04'
            ),
            [])


class ChunkTest(unittest.TestCase):
    def test_chunk(self):
        i = chunk(
            ['1', '2', '3', '4', '5', '6', '7'],
            2)
        self.assertEqual(
            list(i),
            [
                ['1', '2'],
                ['3', '4'],
                ['5', '6'],
                ['7'],
            ])
