import pandas as pd
import re

from datetime import datetime

PATH_TO_DATA = r'C:\Users\Anat\Documents\whatsapp_project_data'

TIME_COL = 'message_time'
SENDER_COL = 'message_sender'
CONTENT_COL = 'message_content'


def parse_message(msg):
    """
    Parses message - time, sender and content.
    Hard-coded for WhatsApp format - "mm/dd/yy, HH:MM - <sender>: <content>"
    :param msg: string
    :return: time (datetime), sender (string) and content (string)
    """
    time_sender_pattern = "[0-9]{1,2}[\/\.][0-9]{1,2}[\/\.][0-9]{2,4}, [0-9]{2}:[0-9]{2} - .+: "
    time_sender_match = re.match(time_sender_pattern, msg)

    if time_sender_match is None:
        # This means the current msg does not have time or sender - it is a continuation of previous message
        return None, None, msg

    time_sender_match_group = time_sender_match.group()
    split_by_dash = time_sender_match_group.split(' - ')
    msg_time, msg_sender = split_by_dash[0:2]

    # this is a heuristic to check if time is in mm/dd/yy format or dd.mm.yyyy format
    if '/' in msg_time:
        time = datetime.strptime(msg_time, '%m/%d/%y, %H:%M')
    elif '.' in msg_time:
        time = datetime.strptime(msg_time, '%d.%m.%Y, %H:%M')
    else:
        raise Exception('Unexpected date and time format - {}'.format(msg_time))

    sender = msg_sender[:-2]  # remove ': ' from end of sender id
    content = msg[len(time_sender_match_group):]
    return time, sender, content


def read_file(path_to_file):
    """
    Reads message file, calls parse_message and populates DataFrame.
    :param path_to_file: string
    :return: DataFrame with columns of TIME_COL, SENDER_COL, CONTENT_COL
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
        data={TIME_COL: time_list, SENDER_COL: sender_list, CONTENT_COL: content_list},
    )


class MessageDatabase(object):
    def __init__(self, path_to_file):
        self.df = read_file(path_to_file)
        self.mapping_dictionary = {}

    def add_from_file(self, path_to_file):
        """
        Adds messages from file path_to_file. Checks for overlap and only adds unique messages.
        Sorts by time and resets index when done.
        Assumption - the new file is consistent, meaning the message time is monotonic (non-decreasing).
        :param path_to_file: string, path to file in correct format
        """
        new_df = read_file(path_to_file)
        # check overlap between messages before loading (using concat problematic, as no keys are available
        # and message time sender and content must be checked)

        # check time overlap
        new_start_time = new_df[TIME_COL].iloc[0]
        new_end_time = new_df[TIME_COL].iloc[-1]

        curr_start_time = self.df[TIME_COL].iloc[0]
        curr_end_time = self.df[TIME_COL].iloc[-1]

        # find overlap between current () and new [] - either ( [ ) ] or [ ( ) ] or ( [ ] ) or [ ( ] )
        # within overlap - check sender and content and only keep new messages with their timestamp

        if new_start_time <= curr_end_time:
            if new_end_time > curr_end_time:
                # overlap is between start of new and end of current ( [ ) ]
                rows_to_add_from_overlap = \
                    MessageDatabase._return_new_not_in_current_overlap(self.df, new_df, new_start_time, curr_end_time)
                rows_to_add_from_new = new_df[new_df[TIME_COL] > curr_end_time]
            else:
                # new time range is included in current time range ( [ ] )
                rows_to_add_from_overlap = \
                    MessageDatabase._return_new_not_in_current_overlap(self.df, new_df, new_start_time, new_end_time)
                rows_to_add_from_new = pd.DataFrame(data={TIME_COL: [], SENDER_COL: [], CONTENT_COL: []})
        else:
            if new_end_time > curr_end_time:
                # current time range is included in new time range [ ( ) ]
                rows_to_add_from_overlap = \
                    MessageDatabase._return_new_not_in_current_overlap(self.df, new_df, curr_start_time, curr_end_time)
                rows_to_add_from_new = pd.concat([new_df[new_df[TIME_COL] > curr_end_time],
                                                  new_df[new_df[TIME_COL] < curr_start_time]])
            else:
                # overlap is between start of current and end of new [ ( ] )
                rows_to_add_from_overlap = \
                    MessageDatabase._return_new_not_in_current_overlap(self.df, new_df, curr_start_time, new_end_time)
                rows_to_add_from_new = new_df[new_df[TIME_COL] < curr_start_time]

        self.df = pd.concat([self.df, rows_to_add_from_overlap, rows_to_add_from_new], ignore_index=True)
        self.df.sort_values(TIME_COL, inplace=True)
        self.df.reindex()

    @staticmethod
    def _return_new_not_in_current_overlap(curr_df, new_df, overlap_start, overlap_end):
        """
        Returns all rows from new df, within time overlap, that don't appear in current df.
        :param curr_df: DataFrame
        :param new_df: DataFrame, may have overlap with curr_df
        :param overlap_start: timestamp, start of overlap
        :param overlap_end: timestamp, end of overlap
        :return: DataFrame, rows from new_df in overlap
        """
        # create mask of overlap time range in both dfs
        curr_mask = (curr_df[TIME_COL] >= overlap_start) & (curr_df[TIME_COL] <= overlap_end)
        new_mask = (new_df[TIME_COL] >= overlap_start) & (new_df[TIME_COL] <= overlap_end)

        # extract overlap from both dfs
        curr_overlap = curr_df.loc[curr_mask]
        new_overlap = new_df.loc[new_mask]

        # create comparison column - time, sender, content
        def create_comparison_column(row):
            return '-'.join([str(row[TIME_COL]), row[SENDER_COL], row[CONTENT_COL]])

        # TODO (low priority): solve the SettingWithCopyWarning here
        curr_overlap['comparison'] = curr_overlap.apply(create_comparison_column, axis=1)
        new_overlap['comparison'] = new_overlap.apply(create_comparison_column, axis=1)

        # return only rows in new_overlap whose values are NOT in the 'comparison' column of current
        new_not_in_curr = ~new_overlap['comparison'].isin(curr_overlap['comparison'])

        new_overlap = new_overlap[new_not_in_curr]
        new_overlap.drop('comparison', axis=1, inplace=True)
        return new_overlap

    def map_senders(self, mapping_dictionary):
        """
        Updates self.df[SENDER_COL] column by using the mapping dictionary and replacing its keys with values in
        that column. Updates self.mapping_dictionary.
        :param mapping_dictionary: dict, keys are strings to be replaced with values (strings)
        """
        self.df[SENDER_COL] = self.df[SENDER_COL].map(mapping_dictionary)
        self.mapping_dictionary.update(mapping_dictionary)

    def show_graphs(self):
        """
        Shows graphs and statistics on self.df
        """
        # TODO: implement private methods for different statistics
        # TODO:(self._most_messages, self._most_words, self._active_time, etc.)
        # TODO: call them here to create a summary report
        pass

    def add_information(self):
        """
        Adds new columns of information for each message in self.df - number of words, sentiment, translation, etc.
        """
        # TODO: implement private methods for different new columns
        # TODO:(number of words, sentiment, translation, etc.)
        # TODO: call them here to create new columns
        pass
