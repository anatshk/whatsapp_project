from unittest import TestCase

from os import path
from pandas.testing import assert_frame_equal

from project_code.data_prep import *


class TestParseMessage(TestCase):
    def test_sanity(self):
        msg = '1/15/18, 11:13 - Bob: blah blah'
        expected_time = datetime(2018, 1, 15, 11, 13)
        expected_sender = 'Bob'
        expected_content = 'blah blah'

        time, sender, content = parse_message(msg)
        self.assertEqual(time, expected_time)
        self.assertEqual(sender, expected_sender)
        self.assertEqual(content, expected_content)

    def test_sender_has_comma(self):
        msg = '1/15/18, 11:13 - Bob, Jones: blah blah'
        expected_time = datetime(2018, 1, 15, 11, 13)
        expected_sender = 'Bob, Jones'
        expected_content = 'blah blah'

        time, sender, content = parse_message(msg)
        self.assertEqual(time, expected_time)
        self.assertEqual(sender, expected_sender)
        self.assertEqual(content, expected_content)

    def test_continuing_message(self):
        msg = 'blah blah'

        time, sender, content = parse_message(msg)
        self.assertIsNone(time)
        self.assertIsNone(sender)
        self.assertEqual(content, msg)

    def test_different_date_format(self):
        msg = '19.5.2016, 20:42 - Bob: blah blah'
        expected_time = datetime(2016, 5, 19, 20, 42)
        expected_sender = 'Bob'
        expected_content = 'blah blah'

        time, sender, content = parse_message(msg)
        self.assertEqual(time, expected_time)
        self.assertEqual(sender, expected_sender)
        self.assertEqual(content, expected_content)

    def test_non_english_in_username(self):
        msg = u'19.5.2016, 20:42 - אדי: איגור מדינה שהנוכחות שלי בה תשמח אותך מאוד'
        expected_time = datetime(2016, 5, 19, 20, 42)
        expected_sender = u'אדי'
        expected_content = u'איגור מדינה שהנוכחות שלי בה תשמח אותך מאוד'

        time, sender, content = parse_message(msg)
        self.assertEqual(time, expected_time)
        self.assertEqual(sender, expected_sender)
        self.assertEqual(content, expected_content)


class TestReadFile(TestCase):
    def test_sanity(self):
        path_to_file = path.join(PATH_TO_DATA, 'tests', '1.txt')
        actual_df = read_file(path_to_file)

        expected_time = [
            datetime(2017, 6, 16, 14, 1),
            datetime(2017, 6, 16, 14, 2),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 3),
            datetime(2017, 6, 16, 14, 4)]
        expected_sender = [
            'bob',
            'alice is the best',
            'charles, you know who he is',
            'charles, you know who he is',
            'charles, you know who he is',
            'bob']
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
        assert_frame_equal(actual_df, expected_df)
