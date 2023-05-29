# words_counter

This API provides a simple and efficient way to count the number of times a word appeared so far (in all previous calls).

## Assumptions
The Words Counter System makes the following assumptions:

1. The input text is in a plain text format.
2. Words are defined as contiguous sequences of characters separated by spaces.
3. Special characters, such as line breaks, tabs, digits, or other non-alphanumeric characters, will not be counted as part of the word and will be cleaned up (eg: what! is equal to what3). 
4. As instruced, case insensitive, dashes and commas are part of the word and would not be cleaned up (eg: what, is not equal to what).
5. In case that the input is a URL address, only the return text will be counted (html tags and etc will be ignored).
6. The system supports in 4 different file types: txt files, json files, csv files and docx files (other file types will not be supported).
7. The system supports multiple languages, as long as the text is written using a standard character encoding.

## Features
The Word Counter System offers the following features:

1. Accurate Word Count: The system accurately counts the number of words in a given text, adhering to the assumptions mentioned above.
2. Words Statistics feature: The system recevies a word and returns the number of times the word appeared so far (in all previous calls).

## Getting Started
1. Clone words_counter project into your computer or server.
2. Make sure all dependencies are installed (requierments.txt file is attached to this repository).
3. Edit db_config.ini file as followed:
  3.1 In line 2 edit DB_USERNAME field with your presonal MySQL username.
  3.2 In line 3 edit DB_PASSWORD field with your presonal MySQL password.
4. Run main.py file.
5. Send requests as instructed on API Documentation file.

## Fututre Work
1. To add Caching mechanism.
2. To add GUI.
3. To add more capabilities (eg: delete words, clean spesific calls etc).
4. Support additional file types.
5. Improve tests coverage.
