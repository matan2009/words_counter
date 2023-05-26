import os
import re
from multiprocessing import Pool
import requests
import csv
import json
from bs4 import BeautifulSoup
import html2text
from fastapi import HTTPException
from requests import HTTPError
from validators import url

from logging import Logger


class WordsCounterHelper:

    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger
        self.words_counter_mapping = {}

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
    def read_text_file(file_path, chunk_size):
        file_content = ""
        # reading text file
        with open(file_path, "r") as file:
            while True:
                chunk_content = file.read(chunk_size)
                if not chunk_content:
                    break
                file_content += chunk_content

        return file_content

    @staticmethod
    def read_csv_file(file_path, chunk_size):
        # reading CSV file
        file_content = ""
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
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

    @staticmethod
    def concatenate_json_data(json_data):
        concatenated = ''
        for key, value in json_data.items():
            concatenated += str(key) + " " + str(value) + " "
        return concatenated

    @staticmethod
    def process_chunk(chunk, converter):
        text = converter.handle(chunk.decode('utf-8'))
        chunked_words = re.findall(r'\b\w+\b', text)
        return chunked_words

    @staticmethod
    def read_json_file(file_path, chunk_size):
        file_content = ""
        # reading JSON file
        with open(file_path, 'r') as file:
            chunk = []
            for line in file:
                chunk.append(line.strip())
                if len(chunk) >= chunk_size:
                    file_content += ' '.join(chunk) + ' '  # Concatenate the chunk
                    chunk = []  # Reset the chunk

            if chunk:
                file_content += ' '.join(chunk) + ' '
        return file_content

    def read_file_content(self, file_path: str) -> str:
        file_content = ""
        chunk_size = 10485760  # 10MB chunk size
        file_extension = file_path.split(".")[-1].lower()

        if file_extension == "txt":
            file_content = self.read_text_file(file_path, chunk_size)

        elif file_extension == "csv":
            file_content = self.read_csv_file(file_path, chunk_size)

        elif file_extension == "json":
            file_content = self.read_json_file(file_path, chunk_size)

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

    def read_url_content(self, url: str):
        try:
            response = requests.get(url, stream=True)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error(f"Failed to retrieve data from URL", extra={"extra": extra_msg})
            raise HTTPException(status_code=404, detail="Request failed")

        try:
            response.raise_for_status()
        except HTTPError:
            self.logger.error(f"The response has wrong status code. Status code is: {response.status_code}")
            raise HTTPException(status_code=response.status_code, detail=response.reason)

        num_workers = 5
        chunk_size = len(response.content) // num_workers
        if not chunk_size:
            chunk_size = 1
        words = []
        converter = html2text.HTML2Text()
        converter.ignore_links = True
        converter.ignore_images = True
        with Pool(processes=num_workers) as pool:
            for chunk in response.iter_content(chunk_size=chunk_size):
                result = pool.starmap(self.process_chunk, [(chunk, converter)])
                words.extend(result[0])
        return words

    def extract_text_from_input(self, received_input: str) -> str or list:
        if os.path.isfile(received_input):
            # the input is a file
            extra_msg = f"file path is: {received_input}"
            self.logger.info("the received input is a path to a file", extra={"extra": extra_msg})
            file_content = self.read_file_content(received_input)
            return file_content

        try:
            validation_result = url(received_input)
            if not validation_result:
                raise ValueError

            # the input is a URL
            extra_msg = f"url is: {received_input}"
            self.logger.info("the received input is URL", extra={"extra": extra_msg})
            url_content = self.read_url_content(received_input)
            return url_content

        except ValueError:
            pass

        # the input is a simple string
        return received_input
