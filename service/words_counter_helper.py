import os
import re
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
            words = list(map(str.lower, retrieved_data))

        for index in range(len(words)):
            # verify if word contains letters
            if bool(re.search(r'[a-zA-Z]', words[index])):
                # cleaning up all characters from word except letters dashes and commas
                words[index] = re.sub(r'[^a-zA-Z,-]', '', words[index])
        return words

    def read_file_content(self, file_path: str) -> str:
        file_content = ""
        file_extension = file_path.split(".")[-1].lower()

        if file_extension == "txt":
            # reading text file
            with open(file_path, "r") as file:
                file_content = file.read()

        elif file_extension == "csv":
            # reading CSV file
            with open(file_path, "r") as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    for item in row:
                        if item:
                            file_content += item + " "

        elif file_extension == "json":
            # reading JSON file
            with open(file_path, "r") as file:
                json_info = json.load(file)
                for key, value in json_info.items():
                    file_content += key + " " + str(value) + " "

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

    def extract_text_from_input(self, received_input: str) -> str or list:
        if os.path.isfile(received_input):
            extra_msg = f"file path is: {received_input}"
            self.logger.info("the received input is a path to a file", extra={"extra": extra_msg})
            file_content = self.read_file_content(received_input)
            return file_content
        try:
            validation_result = url(received_input)
            if not validation_result:
                raise ValueError

            extra_msg = f"url is: {received_input}"
            self.logger.info("the received input is URL", extra={"extra": extra_msg})
            try:
                response = requests.get(received_input)
            except Exception as ex:
                extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                self.logger.error(f"Failed to retrieve data from URL", extra={"extra": extra_msg})
                raise HTTPException(status_code=404, detail="Request failed")

            try:
                response.raise_for_status()
            except HTTPError:
                self.logger.error(f"The response has wrong status code. Status code is: {response.status_code}")
                raise HTTPException(status_code=response.status_code, detail=response.reason)

            html = response.content
            converter = html2text.HTML2Text()
            converter.ignore_links = True
            converter.ignore_images = True
            text = converter.handle(html.decode('utf-8'))
            return re.findall(r'\b\w+\b', text)

        except ValueError:
            pass

        return received_input
