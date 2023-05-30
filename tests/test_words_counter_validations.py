from unittest import TestCase, mock

from main import WordsCounter
from service.words_counter_helper import WordsCounterHelper
from type.response_status import ResponseStatus


class TestWordsCounter(TestCase):

    @mock.patch("configurations.words_counter_configurations.get_configurations")
    def setUp(self, mocked_config) -> None:
        mocked_config.return_value = {"words_counter_helper": {"num_of_workers": 1, "num_of_chunks": 1,
                                                               "files_chunk_size": 1}}
        self.word_counter = WordsCounter(logger=mock.Mock())
        self.helper_instance = WordsCounterHelper(logger=mock.Mock())

    def test_validate_word_statistics_request_success(self):
        word = "what34%"
        res = self.word_counter.validate_word_statistics_request(word)
        self.assertEqual(type(res[0]), ResponseStatus)
        self.assertEqual(res[0], ResponseStatus.Ok)
        self.assertIsNone(res[1])

    def test_validate_word_statistics_request_error(self):
        word = "4576%"
        res = self.word_counter.validate_word_statistics_request(word)
        self.assertEqual(type(res[0]), ResponseStatus)
        self.assertEqual(res[0], ResponseStatus.Error)
        self.assertEqual(res[1], "A requested word must contains at least one letter")
