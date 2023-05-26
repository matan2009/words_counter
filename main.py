from fastapi import FastAPI, Request, HTTPException
import uvicorn
from json import JSONDecodeError

from monitoring.logger import Logger
from service.database_helper import DatabaseHelper
from service.words_counter_helper import WordsCounterHelper
from type.response_status import ResponseStatus

app = FastAPI()


@app.post('/word_counter', response_model=ResponseStatus)
async def word_counter(request: Request) -> ResponseStatus:
    status = await words_counter.set_text_input(request)
    return status


@app.get('/word_statistics/{word}', response_model=int)
def word_statistics(word: str) -> int:
    word_count = words_counter.get_word_statistics(word)
    return word_count


class WordsCounter:

    def __init__(self, logger: Logger):
        self.logger = logger.logger
        self.words_counter_helper = WordsCounterHelper(logger.logger)
        self.database_helper = DatabaseHelper()

    @staticmethod
    async def validate_request(request: Request) -> (ResponseStatus, str):
        content_type = request.headers.get('Content-Type')
        if not content_type:
            return ResponseStatus.Error, 'No Content-Type provided.'
        elif content_type == 'application/json':
            try:
                json = await request.json()
                if not type(json) == dict:
                    return ResponseStatus.Error, 'Content-Type not supported.'
                received_input = json.get("received_input")
                if not received_input:
                    return ResponseStatus.Error, "The request contains unexpected keys"
                elif type(received_input) != str:
                    return ResponseStatus.Error, "The received input type must to be a string"
                return ResponseStatus.Ok, received_input
            except JSONDecodeError:
                return ResponseStatus.Error, 'Invalid JSON data.'
        else:
            return ResponseStatus.Error, 'Content-Type not supported.'

    async def set_text_input(self, request: Request) -> ResponseStatus:
        validation_status, received_input = await self.validate_request(request)
        if validation_status == ResponseStatus.Error:
            extra_msg = f"error is: {received_input}"
            self.logger.error("the request validation was failed", extra={"extra": extra_msg})
            raise HTTPException(status_code=400, detail=received_input)

        extra_msg = f"received_input is: {received_input}"
        self.logger.info(f"got a request to count the number of appearances for each word in the input",
                         extra={"extra": extra_msg})
        try:
            retrieved_data = self.words_counter_helper.extract_text_from_input(received_input)
            if not retrieved_data:
                self.logger.critical(f"could not extract text from input", extra={"extra": extra_msg})
                raise HTTPException(status_code=400, detail="Bad Request")

        except HTTPException as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.critical(f"an http exception occurred while trying to extract text from input", extra={"extra": extra_msg})
            raise ex

        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.critical(f"an unexpected error occurred while trying to extract text from input", extra={"extra": extra_msg})
            raise HTTPException(status_code=500, detail=str(ex))

        words = self.words_counter_helper.prepare_words_for_counting(retrieved_data)
        words_counter_mapping = self.words_counter_helper.process_words(words)
        try:
            self.database_helper.update_database(words_counter_mapping)
            status = ResponseStatus.Ok
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.critical(f"an error occurred while trying to update database", extra={"extra": extra_msg})
            status = ResponseStatus.Error
        finally:
            self.words_counter_helper.clean_words_counter_mapping()
        return status

    def get_word_statistics(self, word: str):
        # todo: cleanup words
        # todo: check why still there are results after deleting all rows on db
        extra_msg = f"word is: {word}"
        self.logger.info("got a request to get word statistics", extra={"extra": extra_msg})
        try:
            count = self.database_helper.get_count_from_db(word)
        except Exception as ex:
            extra_msg = f"word is: {word}, the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.critical(f"an error occurred while trying to get word count from database",
                                 extra={"extra": extra_msg})
            raise HTTPException(status_code=500)
        return count


if __name__ == '__main__':
    logger = Logger()
    words_counter = WordsCounter(logger)
    words_counter.database_helper.verify_db()
    uvicorn.run(app, host="0.0.0.0", port=8000)
