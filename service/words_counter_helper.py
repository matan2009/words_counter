from logging import Logger


class WordsCounterHelper:

    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger
        self.words_counter_mapping = {}

    def update_words_counter_mapping(self, words: list):
        for word in words:
            if word in self.words_counter_mapping:
                self.words_counter_mapping[word] += 1
            else:
                self.words_counter_mapping[word] = 1

    def clean_words_counter_mapping(self):
        self.words_counter_mapping.clear()
