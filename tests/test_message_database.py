from unittest import TestCase

from datetime import datetime
from os import path
from pandas.testing import assert_frame_equal

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
