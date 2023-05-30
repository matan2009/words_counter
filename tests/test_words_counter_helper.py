from unittest import TestCase, mock

from service.database_helper import DatabaseHelper
from service.words_counter_helper import WordsCounterHelper


class TestWordsCounterHelper(TestCase):

    @mock.patch("configurations.words_counter_configurations.get_configurations")
    def setUp(self, mocked_config) -> None:
        mocked_config.return_value = {"words_counter_helper": {"num_of_workers": 1, "num_of_chunks": 1,
                                                               "files_chunk_size": 1}}
        self.database_helper = DatabaseHelper()
        self.helper_instance = WordsCounterHelper(logger=mock.Mock())

    def test_extract_text_from_input_string(self):
        input_string = "simple string"
        self.helper_instance.process_file_content = mock.Mock()
        self.helper_instance.process_url_content = mock.Mock()
        self.helper_instance.update_words_counter_mapping = mock.Mock()
        self.helper_instance.extract_text_from_input(input_string)
        self.helper_instance.process_file_content.assert_not_called()
        self.helper_instance.process_url_content.assert_not_called()
        self.helper_instance.update_words_counter_mapping.assert_called_once_with(input_string.split())

    def test_extract_text_from_non_exist_file(self):
        input_string = "C:/Path/To/A/file.txt"
        self.helper_instance.process_file_content = mock.Mock()
        self.helper_instance.process_url_content = mock.Mock()
        self.helper_instance.update_words_counter_mapping = mock.Mock()
        try:
            self.helper_instance.extract_text_from_input(input_string)
        except ValueError as ex:
            self.assertEqual(type(ex), ValueError)
            self.assertEqual(str(ex), "The request contains a path to a file that does not exist")
        self.helper_instance.process_file_content.assert_not_called()
        self.helper_instance.process_url_content.assert_not_called()
        self.helper_instance.update_words_counter_mapping.assert_not_called()

    def test_extract_text_from_url(self):
        input_string = "https://google.com"
        self.helper_instance.process_file_content = mock.Mock()
        self.helper_instance.process_url_content = mock.Mock()
        self.helper_instance.update_words_counter_mapping = mock.Mock()
        self.helper_instance.extract_text_from_input(input_string)
        self.helper_instance.process_file_content.assert_not_called()
        self.helper_instance.process_url_content.assert_called_with(input_string)
        self.helper_instance.update_words_counter_mapping.assert_not_called()