import json


def get_configurations():
    with open("config.json", "r") as config_file:
        config = json.load(config_file)

    return config


class WordsCounterConfigurations:

    def __init__(self):
        self.config = get_configurations()
