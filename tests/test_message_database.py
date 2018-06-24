from unittest import TestCase

from datetime import datetime
from os import path
from pandas.testing import assert_frame_equal, assert_series_equal

import pandas as pd

from project_code.data_prep import MessageDatabase, PATH_TO_DATA


class TestMessageDatabase(TestCase):
    def test_init(self):
        pth = path.join(PATH_TO_DATA, 'WhatsApp Chat with cheches friends 1.txt')

        new_db = MessageDatabase(path_to_file=pth)

        expected_first_line = pd.DataFrame(
            data={'message_time': [datetime(2016, 5, 19, 20, 41)],
                  'message_sender': ['‪+1 (434) 227-2185‬'],
                  'message_content': [u'שוב מוכר נשק למשטרים דיקטטוריים']},
        )
        expected_last_line = pd.DataFrame(
            data={'message_time': [datetime(2018, 1, 15, 10, 47)],
                  'message_sender': ['‪+972 50-686-1962‬‬'],
                  'message_content': [u'תודה!']},
        )

        self.assertEqual(len(new_db.df), 42825)
        assert_frame_equal(new_db.df.iloc[0], expected_first_line)
        assert_frame_equal(new_db.df.iloc[-1], expected_last_line)

    def test_map_senders(self):
        pth = path.join(PATH_TO_DATA, 'tests', '1.txt')

        new_db = MessageDatabase(path_to_file=pth)

        change_names = {
            'bob': 'robert',
            'alice is the best': 'alice richards',
            'charles, you know who he is': 'that guy'
        }

        new_db.map_senders(mapping_dictionary=change_names)

        expected_time = [
            datetime(2017, 6, 16, 14, 1),
            datetime(2017, 6, 16, 14, 2),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 4)]
        expected_sender = [
            'robert',
            'alice richards',
            'that guy',
            'that guy',
            'that guy',
            'robert']
        expected_content = [
            'hello',
            'hi',
            'first message row',
            'second message row',
            'charles is done speaking...',
            'hello again',
        ]
        expected_df = pd.DataFrame(
            data={'message_time': expected_time, 'message_sender': expected_sender, 'message_content': expected_content},
        )
        assert_frame_equal(new_db.df, expected_df)
        self.assertDictEqual(new_db.mapping_dictionary, change_names)

    def test_return_new_not_in_current_overlap_1(self):
        # Test [ ( ] ) - overlap between start of left and end of right
        data_left = {
            'message_content': ['a', 'b', 'c', 'd', 'e'],
            'message_sender': ['a', 'b', 'c', 'd', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 10, 0),
                datetime(2000, 1, 1, 10, 5),
                datetime(2000, 1, 1, 10, 10),
                datetime(2000, 1, 1, 10, 15),
                datetime(2000, 1, 1, 10, 20),
            ]
        }
        data_right = {
            'message_content': ['a', 'b', 'c', 'd', 'e'],
            'message_sender': ['a', 'b', 'c', 'd', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 9, 0),  # too early
                datetime(2000, 1, 1, 9, 30),  # too early
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 15),  # same time, same sender and content
                datetime(2000, 1, 1, 10, 18),  # same sender, different time
            ]
        }

        expected = pd.DataFrame({
            'message_content': ['c', 'e'],
            'message_sender': ['c', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 18),
            ]
        }, index=[2, 4])

        left = pd.DataFrame(data_left)
        right = pd.DataFrame(data_right)
        start = datetime(2000, 1, 1, 10, 5)
        end = datetime(2000, 1, 1, 10, 18)
        actual = MessageDatabase._return_new_not_in_current_overlap(left, right, start, end)
        assert_frame_equal(actual, expected)

    def test_return_new_not_in_current_overlap_2(self):
        # Test [ ( ) ] - overlap between start of left and end of left
        data_left = {
            'message_content': ['a', 'b', 'c', 'd', 'e'],
            'message_sender': ['a', 'b', 'c', 'd', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 10, 0),
                datetime(2000, 1, 1, 10, 5),
                datetime(2000, 1, 1, 10, 10),
                datetime(2000, 1, 1, 10, 15),
                datetime(2000, 1, 1, 10, 20),
            ]
        }
        data_right = {
            'message_content': ['a', 'b', 'c', 'd', 'e', 'f'],
            'message_sender': ['a', 'b', 'c', 'd', 'e', 'f'],
            'message_time': [
                datetime(2000, 1, 1, 9, 0),  # too early
                datetime(2000, 1, 1, 9, 30),  # too early
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 15),  # same time, same sender and content
                datetime(2000, 1, 1, 10, 18),  # same sender, different time
                datetime(2000, 1, 1, 10, 50),  # too late
            ]
        }

        expected = pd.DataFrame({
            'message_content': ['c', 'e'],
            'message_sender': ['c', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 18),
            ]
        }, index=[2, 4])

        left = pd.DataFrame(data_left)
        right = pd.DataFrame(data_right)
        start = datetime(2000, 1, 1, 10, 5)
        end = datetime(2000, 1, 1, 10, 18)
        actual = MessageDatabase._return_new_not_in_current_overlap(left, right, start, end)
        assert_frame_equal(actual, expected)

    def test_return_new_not_in_current_overlap_3(self):
        # Test ( [ ) ] - overlap between start of right and end of left
        data_left = {
            'message_content': ['z', 'x', 'a', 'b', 'c', 'd', 'e'],
            'message_sender': ['z', 'x', 'a', 'b', 'c', 'd', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 8, 5),
                datetime(2000, 1, 1, 8, 30),
                datetime(2000, 1, 1, 10, 0),
                datetime(2000, 1, 1, 10, 5),
                datetime(2000, 1, 1, 10, 10),
                datetime(2000, 1, 1, 10, 15),
                datetime(2000, 1, 1, 10, 20),
            ]
        }
        data_right = {
            'message_content': ['a', 'b', 'c', 'd', 'e', 'f'],
            'message_sender': ['a', 'b', 'c', 'd', 'e', 'f'],
            'message_time': [
                datetime(2000, 1, 1, 9, 0),  # too early
                datetime(2000, 1, 1, 9, 30),  # too early
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 15),  # same time, same sender and content
                datetime(2000, 1, 1, 10, 18),  # same sender, different time
                datetime(2000, 1, 1, 10, 50),  # too late
            ]
        }

        expected = pd.DataFrame({
            'message_content': ['c', 'e'],
            'message_sender': ['c', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 18),
            ]
        }, index=[2, 4])

        left = pd.DataFrame(data_left)
        right = pd.DataFrame(data_right)
        start = datetime(2000, 1, 1, 10, 5)
        end = datetime(2000, 1, 1, 10, 18)
        actual = MessageDatabase._return_new_not_in_current_overlap(left, right, start, end)
        assert_frame_equal(actual, expected)

    def test_return_new_not_in_current_overlap_4(self):
        # Test ( [ ] ) - overlap between start of right and end of right
        data_left = {
            'message_content': ['z', 'x', 'a', 'b', 'c', 'd', 'e', 'f', 'g'],
            'message_sender': ['z', 'x', 'a', 'b', 'c', 'd', 'e', 'f', 'g'],
            'message_time': [
                datetime(2000, 1, 1, 8, 5),
                datetime(2000, 1, 1, 8, 30),
                datetime(2000, 1, 1, 10, 0),
                datetime(2000, 1, 1, 10, 5),
                datetime(2000, 1, 1, 10, 10),
                datetime(2000, 1, 1, 10, 15),
                datetime(2000, 1, 1, 10, 20),
                datetime(2000, 1, 1, 13, 20),
                datetime(2000, 1, 1, 13, 22),
            ]
        }
        data_right = {
            'message_content': ['a', 'b', 'c', 'd', 'e'],
            'message_sender': ['a', 'b', 'c', 'd', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 9, 0),  # too early
                datetime(2000, 1, 1, 9, 30),  # too early
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 15),  # same time, same sender and content
                datetime(2000, 1, 1, 10, 18),  # same sender, different time
            ]
        }

        expected = pd.DataFrame({
            'message_content': ['c', 'e'],
            'message_sender': ['c', 'e'],
            'message_time': [
                datetime(2000, 1, 1, 10, 5),  # same time, different sender
                datetime(2000, 1, 1, 10, 18),
            ]
        }, index=[2, 4])

        left = pd.DataFrame(data_left)
        right = pd.DataFrame(data_right)
        start = datetime(2000, 1, 1, 10, 5)
        end = datetime(2000, 1, 1, 10, 18)
        actual = MessageDatabase._return_new_not_in_current_overlap(left, right, start, end)
        assert_frame_equal(actual, expected)

    def test_add_from_file(self):
        pth1 = path.join(PATH_TO_DATA, 'tests', '1.txt')
        pth2 = path.join(PATH_TO_DATA, 'tests', '2.txt')

        db = MessageDatabase(pth1)
        db.add_from_file(pth2)

        expected_time = [
            datetime(2017, 6, 16, 14, 1),
            datetime(2017, 6, 16, 14, 2),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 4),
            datetime(2017, 6, 16, 14, 5),
            datetime(2017, 6, 16, 15, 8),
            datetime(2017, 6, 16, 15, 10),
            datetime(2017, 6, 16, 15, 14),
        ]
        expected_sender = [
            'bob',
            'alice is the best',
            'charles, you know who he is',
            'charles, you know who he is',
            'charles, you know who he is',
            'bob',
            'bob',
            'dave',
            'bob',
            'dave'
        ]
        expected_content = [
            'hello',
            'hi',
            'first message row',
            'second message row',
            'charles is done speaking...',
            'hi',
            'hello again',
            'what did i miss?',
            'nothing',
            'great!'
        ]
        expected_df = pd.DataFrame(
            data={'message_time': expected_time, 'message_sender': expected_sender,
                  'message_content': expected_content},
        )
        assert_frame_equal(db.df, expected_df)

    def test_count_words(self):
        pth = path.join(PATH_TO_DATA, 'tests', '1.txt')
        db = MessageDatabase(pth)
        word_count = db._count_words()
        expected = pd.Series([1, 1, 3, 3, 4, 1, 2])
        assert_series_equal(word_count, expected)

    def test_count_special_punctuation(self):
        pth = path.join(PATH_TO_DATA, 'tests', '2.txt')
        db = MessageDatabase(pth)
        punctuation_count = db._count_special_punctuation()
        expected = pd.Series([0, 0, 1, 0, 1])
        assert_series_equal(punctuation_count, expected)

    def test_time_bin(self):
        pth = path.join(PATH_TO_DATA, 'tests', '2.txt')
        db = MessageDatabase(pth)
        time_bin = db._time_bin()
        expected = pd.Series(['14:00', '14:00', '15:00', '15:00', '15:00'])
        assert_series_equal(time_bin, expected)

    def test_time_diff(self):
        pth = path.join(PATH_TO_DATA, 'tests', '2.txt')
        db = MessageDatabase(pth)
        time_diff = db._time_diff()
        expected = pd.Series([pd.nan, 1, 63, 2, 4])
        assert_series_equal(time_diff, expected)
