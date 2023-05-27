import os
import re
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Pool
import requests
import csv
import docx2txt
import html2text
from fastapi import HTTPException
from requests import HTTPError

from logging import Logger
from configurations.words_counter_configurations import WordsCounterConfigurations


class WordsCounterHelper(WordsCounterConfigurations):

    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger
        self.words_counter_mapping = {}
        self.txt_extension = "txt"
        self.csv_extension = "csv"
        self.json_extension = "json"
        self.docx_extension = "docx"
        self.file_path_pattern = r'^[A-Za-z]:/(?:[^<>:"/\\|?*]+/)*[^<>:"/\\|?*]+\.[A-Za-z]+$'
        self.url_pattern = r'^(https?|ftp)://[^\s/$.?#].[^\s]*$'

    @staticmethod
    def prepare_words_for_counting(retrieved_data) -> list:
        if isinstance(retrieved_data, str):
            retrieved_data = retrieved_data.lower()  # words are Case insensitive
            words = retrieved_data.split()
        else:  # retrieved_data has type of list
            words = list(map(str.lower, retrieved_data))  # words are Case insensitive

        cleaned_words = []
        for word in words:
            # verify if word contains letters
            if bool(re.search(r'[a-zA-Z]', word)):
                # cleaning up all characters from word except letters dashes and commas
                clean_word = re.sub(r'[^a-zA-Z,-]', '', word)
                if clean_word:
                    cleaned_words.append(clean_word)

        return cleaned_words

    @staticmethod
    def prepare_word_for_statistics(word) -> str:
        # cleaning up all characters from word except letters dashes and commas
        return re.sub(r'[^a-zA-Z,-]', '', word)

    @staticmethod
    def concatenate_json_data(json_data: dict) -> str:
        concatenated = ""
        for key, value in json_data.items():
            concatenated += str(key) + " " + str(value) + " "
        return concatenated

    def read_text_file(self, file_path: str, chunk_size: int) -> str:
        file_content = ""
        # reading text file
        with open(file_path, "r") as file:
            while True:
                try:
                    chunk_content = file.read(chunk_size)
                    if not chunk_content:
                        break
                    file_content += chunk_content
                except Exception as ex:
                    extra_msg = f"chunk_size is: {chunk_size}, the exception is: {str(ex)}, " \
                                f"the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to read a text file",
                                      extra={"extra": extra_msg})
        return file_content

    def read_csv_file(self, file_path: str, chunk_size: int) -> str:
        # reading CSV file
        file_content = ""
        with open(file_path, 'r') as file:
            try:
                reader = csv.reader(file)
            except Exception as ex:
                extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                self.logger.error("an error occurred while trying to read a csv file",
                                  extra={"extra": extra_msg})
                raise ex

            chunk = []
            for row in reader:
                chunk.append(' '.join(row))
                if len(chunk) >= chunk_size:
                    file_content += ' '.join(chunk) + ' '
                    chunk = []

            if chunk:
                # Concatenating the chunk
                file_content += ' '.join(chunk) + ' '
        return file_content

    def read_json_file(self, file_path: str, chunk_size: int) -> str:
        file_content = ""
        # reading JSON file
        with open(file_path, 'r') as file:
            chunk = []
            for line in file:
                try:
                    chunk.append(line.strip())
                    if len(chunk) >= chunk_size:
                        file_content += ' '.join(chunk) + ' '  # Concatenate the chunk
                        chunk = []  # Reset the chunk
                except Exception as ex:
                    extra_msg = f"chunk_size is: {chunk_size}, the exception is: {str(ex)}, " \
                                f"the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to read a json file",
                                      extra={"extra": extra_msg})

            if chunk:
                file_content += ' '.join(chunk) + ' '
        return file_content

    def read_docx_file(self, file_path: str, chunk_size: int) -> str:
        file_content = ""
        # reading docx file
        try:
            text = docx2txt.process(file_path)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error("an error occurred while trying to read a docx file",
                              extra={"extra": extra_msg})
            raise ex

        for i in range(0, len(text), chunk_size):
            file_content += text[i:i + chunk_size]
        return file_content

    @staticmethod
    def process_chunk(chunk, converter, decode_method):
        text = converter.handle(chunk.decode(decode_method))
        # extracting words from text without html tags
        chunked_words = re.findall(r'\b\w+\b', text)
        return chunked_words

    def process_words(self, words: list) -> dict:
        num_of_workers = self.config["words_counter_helper"]["num_of_workers"]
        chunk_size = len(words) // num_of_workers
        if not chunk_size:
            chunk_size = 1
        chunks = [words[i:i+chunk_size] for i in range(0, len(words), chunk_size)]
        with ThreadPoolExecutor(max_workers=num_of_workers) as executor:
            for chunk in chunks:
                try:
                    executor.submit(self.update_words_counter_mapping, chunk)
                except Exception as ex:
                    extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to update words counter mapping",
                                      extra={"extra": extra_msg})
        return self.words_counter_mapping

    def read_file_content(self, file_path: str) -> str:
        file_content = ""
        chunk_size = self.config["words_counter_helper"]["files_chunk_size"]  # 10MB chunk size
        file_extension = file_path.split(".")[-1].lower()

        if file_extension == self.txt_extension:
            file_content = self.read_text_file(file_path, chunk_size)

        elif file_extension == self.csv_extension:
            file_content = self.read_csv_file(file_path, chunk_size)

        elif file_extension == self.json_extension:
            file_content = self.read_json_file(file_path, chunk_size)

        elif file_extension == self.docx_extension:
            file_content = self.read_docx_file(file_path, chunk_size)

        else:
            # handling unsupported file type
            extra_msg = f"file path is {file_path}"
            self.logger.error("cannot read file content from unsupported file type", extra={"extra": extra_msg})

        return file_content

    def update_words_counter_mapping(self, words: list):
        for word in words:
            if word in self.words_counter_mapping:
                self.words_counter_mapping[word] += 1
            else:
                self.words_counter_mapping[word] = 1

    def clean_words_counter_mapping(self):
        self.words_counter_mapping.clear()

    def read_url_content(self, url: str) -> list:
        try:
            response = requests.get(url, stream=True)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error(f"Failed to retrieve data from URL", extra={"extra": extra_msg})
            raise HTTPException(status_code=404, detail="Not Found")

        try:
            response.raise_for_status()
        except HTTPError:
            self.logger.error(f"The response has wrong status code. Status code is: {response.status_code}")
            raise HTTPException(status_code=response.status_code, detail=response.reason)

        num_workers = self.config["words_counter_helper"]["num_of_workers"]
        chunk_size = len(response.content) // num_workers
        if not chunk_size:
            chunk_size = 1
        words = []
        converter = html2text.HTML2Text()
        converter.ignore_links = True
        converter.ignore_images = True
        with Pool(processes=num_workers) as pool:
            for chunk in response.iter_content(chunk_size=chunk_size):
                try:
                    result = pool.starmap(self.process_chunk, [(chunk, converter, response.encoding)])
                    words.extend(result[0])
                except Exception as ex:
                    extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to read url content", extra={"extra": extra_msg})
        return words

    def extract_text_from_input(self, input_string: str) -> str or list:
        # checking if input_string has file path pattern
        file_path_match = re.match(self.file_path_pattern, input_string)
        if file_path_match:
            # this input is a path to a file
            if os.path.isfile(input_string):
                # this file path is a path of an exiting file
                extra_msg = f"file path is: {input_string}"
                self.logger.info("the received input is a valid path to a file", extra={"extra": extra_msg})
                file_content = self.read_file_content(input_string)
                return file_content
            raise HTTPException(status_code=400, detail="The request contains a path to a file that does not exist")

        # checking if input_string has URL address pattern
        url_match = re.match(self.url_pattern, input_string)
        if url_match:
            # this input is a URL address
            extra_msg = f"url is: {input_string}"
            self.logger.info("the received input has URL pattern", extra={"extra": extra_msg})
            url_content = self.read_url_content(input_string)
            return url_content

        # the input is a simple string
        extra_msg = f"string is: {input_string}"
        self.logger.info("the received input is a simple string", extra={"extra": extra_msg})
        return input_string
