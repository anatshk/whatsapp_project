import pandas as pd
import re

from datetime import datetime

PATH_TO_DATA = r'C:\Users\Anat\Documents\whatsapp_project_data'


def parse_message(msg):
    """
    Parses message - time, sender and content.
    Hard-coded for WhatsApp format - "mm/dd/yy, HH:MM - <sender>: <content>"
    :param msg: string
    :return: time (datetime), sender (string) and content (string)
    """
    time_sender_pattern = "[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2}, [0-9]{2}:[0-9]{2} - [a-zA-Z ,]+: "
    time_sender_match = re.match(time_sender_pattern, msg)

    if time_sender_match is None:
        # This means the current msg does not have time or sender - it is a continuation of previous message
        return None, None, msg

    time_sender_match_group = time_sender_match.group()
    time_sender = time_sender_match_group.split(' - ')
    time = datetime.strptime(time_sender[0], '%m/%d/%y, %H:%M')
    sender = time_sender[1][:-2]
    content = msg[len(time_sender_match_group):]
    return time, sender, content


def read_file(path_to_file):
    """
    Reads message file, calls parse_message and populates DataFrame.
    :param path_to_file: string
    :return: DataFrame with columns of 'message_time', 'message_sender', 'message_content'
    """

    data_list = []  # will be a list of tuples (time, sender, content)

    with open(path_to_file, 'rb') as f:
        for line in f:
            time, sender, content = parse_message(str(line, 'utf-8'))
            if time is None and sender is None:
                # this message is a continuation of previous message, copy last time and sender
                time, sender = data_list[-1][0:2]
            data_list.append((time, sender, content.strip()))

    time_list, sender_list, content_list = zip(*data_list)
    return pd.DataFrame(
        data={'message_time': time_list, 'message_sender': sender_list, 'message_content': content_list},
    )


class MessageDatabase(object):
    def __init__(self, path_to_file):
        self.df = read_file(path_to_file)

    def add_from_file(self, path_to_file):
        new_df = read_file(path_to_file)
        # TODO: check overlap between messages before loading
        self.df.append(new_df)

    def show_graphs(self):
        """
        Shows graphs and statistics on self.df
        """
        # TODO: implement private methods for different statistics
        # TODO:(self._most_messages, self._most_words, self._active_time, etc.)
        # TODO: call them here to create a summary report
        pass