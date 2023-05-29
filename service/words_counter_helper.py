from docx import Document
import html2text
from fastapi import HTTPException
import requests
from concurrent.futures.thread import ThreadPoolExecutor

import os
import re
import csv

from logging import Logger
from configurations.words_counter_configurations import WordsCounterConfigurations
from service.database_helper import DatabaseHelper
from type.response_status import ResponseStatus


class WordsCounterHelper(WordsCounterConfigurations):

    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger
        self.database_helper = DatabaseHelper()
        self.words_counter_mapping = {}
        self.cleaned_words = []
        self.txt_extension = "txt"
        self.csv_extension = "csv"
        self.json_extension = "json"
        self.docx_extension = "docx"
        self.file_path_pattern = r'^[A-Za-z]:/(?:[^<>:"/\\|?*]+/)*[^<>:"/\\|?*]+\.[A-Za-z]+$'
        self.url_pattern = r'^(https?|ftp)://[^\s/$.?#].[^\s]*$'

    @staticmethod
    def prepare_word_for_statistics(word: str) -> str:
        # cleaning up all characters from word except letters dashes and commas
        return re.sub(r'[^a-zA-Z,-]', '', word)

    def setup_system(self):
        server_conn, host, user_name, password = self.database_helper.create_connection_to_mysql_server()
        self.database_helper.verify_database(server_conn)
        db_conn = self.database_helper.create_connection_to_database(host, user_name, password)
        self.database_helper.verify_table(db_conn)

    def extract_text_from_input(self, input_string: str):
        # checking if input_string has file path pattern
        file_path_match = re.match(self.file_path_pattern, input_string)
        if file_path_match:
            # this input is a path to a file
            if os.path.isfile(input_string):
                # this file path is a path of an exiting file
                extra_msg = f"file path is: {input_string}"
                self.logger.info("the received input is a valid path to a file", extra={"extra": extra_msg})
                self.process_file_content(input_string)
                return None
            extra_msg = "The request contains a path to a file that does not exist"
            self.logger.warning("file does not exist", extra={"extra": extra_msg})
            raise ValueError(extra_msg)

        # checking if input_string has URL address pattern
        url_match = re.match(self.url_pattern, input_string)
        if url_match:
            # this input is a URL address
            extra_msg = f"url is: {input_string}"
            self.logger.info("the received input has URL pattern", extra={"extra": extra_msg})
            self.process_url_content(input_string)
            return None

        # the input is a simple string
        extra_msg = f"string is: {input_string}"
        self.logger.info("the received input is a simple string", extra={"extra": extra_msg})
        self.update_words_counter_mapping(input_string.split())

    def process_text_file(self, file, chunk: (int, int), chunk_size: int):
        file.seek(chunk[0])
        chunk_size_iteration = chunk_size // 100  # each worker will process 100KB of data on each iteration
        while file.tell() < chunk[1]:
            chunk_content = file.read(chunk_size_iteration)
            if not chunk_content:
                break
            self.update_words_counter_mapping(chunk_content.split())

    def read_text_from_file(self, file, chunk_size: int):
        file_size = file.seek(0, 2)
        num_of_workers = file_size // chunk_size
        if not num_of_workers:
            num_of_workers = 1
        chunks = [(i, i + chunk_size) for i in range(0, file_size, chunk_size)]
        with ThreadPoolExecutor(max_workers=num_of_workers) as executor:
            for chunk in chunks:
                try:
                    executor.submit(self.process_text_file, file, chunk, chunk_size)
                except Exception as ex:
                    extra_msg = f"chunk_size is: {chunk_size}, the exception is: {str(ex)}, " \
                                f"the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to extract text from a text file",
                                      extra={"extra": extra_msg})

    def process_csv_file(self, line: str):
        line_words = []
        for phrase in line:
            line_words.extend(phrase.split())
        self.update_words_counter_mapping(line_words)

    def read_csv_file(self, file, num_of_workers: int):
        # reading CSV file
        reader = csv.reader(file)
        with ThreadPoolExecutor(max_workers=num_of_workers) as executor:
            for line in reader:
                try:
                    executor.submit(self.process_csv_file, line)
                except Exception as ex:
                    extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to process csv file",
                                      extra={"extra": extra_msg})

    def read_docx_file(self, file_path: str, num_of_workers: int):
        # reading docx file
        document = Document(file_path)
        with ThreadPoolExecutor(max_workers=num_of_workers) as executor:
            for paragraph in document.paragraphs:
                try:
                    executor.submit(self.update_words_counter_mapping, paragraph.text.split())
                except Exception as ex:
                    extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to process docx file",
                                      extra={"extra": extra_msg})

    def process_file_content(self, file_path: str):
        supported_file_extensions = [self.txt_extension, self.csv_extension, self.json_extension, self.docx_extension]
        file_extension = file_path.split(".")[-1].lower()
        if file_extension not in supported_file_extensions:
            # handling unsupported file type
            extra_msg = f"file path is {file_path}"
            self.logger.error("cannot read file content from unsupported file type", extra={"extra": extra_msg})
            raise ValueError("Unsupported file type")

        chunk_size = self.config["words_counter_helper"]["files_chunk_size"]  # 10MB chunk size
        num_of_workers = self.config["words_counter_helper"]["num_of_workers"]
        file = open(file_path, "r")
        file_extension = file_path.split(".")[-1].lower()

        if file_extension in [self.txt_extension, self.json_extension]:
            self.read_text_from_file(file, chunk_size)

        elif file_extension == self.csv_extension:
            self.read_csv_file(file, num_of_workers)

        elif file_extension == self.docx_extension:
            self.read_docx_file(file_path, num_of_workers)

        file.close()

    def read_url_content(self, chunk: bytes, converter, decode_method: str):
        text = converter.handle(chunk.decode(decode_method))
        # extracting words from text without html tags
        chunk_content = re.findall(r'\b\w+\b', text)
        self.update_words_counter_mapping(chunk_content)

    def process_url_content(self, url: str):
        try:
            response = requests.get(url, stream=True)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error(f"Failed to retrieve data from URL", extra={"extra": extra_msg})
            raise HTTPException(status_code=404, detail="Not Found")

        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            extra_msg = f"Status code is: {response.status_code}, the exception is: {str(ex)}, " \
                        f"the exception_type is: {type(ex).__name__}"
            self.logger.error(f"The response has wrong status code", extra={"extra": extra_msg})
            raise HTTPException(status_code=response.status_code, detail=response.reason)

        num_workers = self.config["words_counter_helper"]["num_of_workers"]
        chunk_size = len(response.content) // num_workers
        if not chunk_size:
            chunk_size = 1
        converter = html2text.HTML2Text()
        converter.ignore_links = True
        converter.ignore_images = True
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            for chunk in response.iter_content(chunk_size=chunk_size):
                try:
                    executor.submit(self.read_url_content, chunk, converter, response.encoding)
                except Exception as ex:
                    extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                    self.logger.error("an error occurred while trying to read url content", extra={"extra": extra_msg})

    def update_words_counter_mapping(self, words: list):
        for word in words:
            # verify if word contains letters
            if not bool(re.search(r'[a-zA-Z]', word)):
                continue
            # cleaning up all characters from word except letters dashes and commas
            word = re.sub(r'[^a-zA-Z,-]', '', word).lower()

            if word in self.words_counter_mapping:
                self.words_counter_mapping[word] += 1
            else:
                self.words_counter_mapping[word] = 1

    def update_database(self) -> ResponseStatus:
        items = list(self.words_counter_mapping.items())
        num_of_chunks = self.config["words_counter_helper"]["num_of_chunks"]
        items_length = len(items)
        chunk_size = items_length // num_of_chunks
        if not chunk_size:
            chunk_size = items_length
        chunk_update_successfully, chunk_update_failed = False, False
        for i in range(0, len(items), chunk_size):
            try:
                chunk = dict(items[i:i + chunk_size])
                self.database_helper.update_database(chunk)
                chunk_update_successfully = True
            except Exception as ex:
                extra_msg = f"chunk_size is: {chunk_size}, the exception is: {str(ex)}, " \
                            f"the exception_type is: {type(ex).__name__}"
                self.logger.error("an error occurred while trying to update database", extra={"extra": extra_msg})
                chunk_update_failed = True

        if chunk_update_successfully and chunk_update_failed:
            update_status = ResponseStatus.Partial
        elif chunk_update_successfully and not chunk_update_failed:
            update_status = ResponseStatus.Ok
        else:  # all chunks were failed to be updated in DB
            update_status = ResponseStatus.Error
        self.words_counter_mapping.clear()  # clearing words_counter_mapping for the next api call

        return update_status

    def get_word_count(self, word: str) -> int:
        return self.database_helper.get_count_from_db(word)
