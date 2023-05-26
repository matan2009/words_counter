from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from json import JSONDecodeError

from monitoring.logger import create_logger
from service.database_helper import DatabaseHelper
from service.words_counter_helper import WordsCounterHelper
from type.response_status import ResponseStatus

app = FastAPI()


async def validate_request(request):
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
            return 'Invalid JSON data.'
    else:
        return ResponseStatus.Error, 'Content-Type not supported.'


@app.post('/word_counter')
async def word_counter(request: Request) -> ResponseStatus or HTTPException:
    validation_status, received_input = await validate_request(request)
    if validation_status == ResponseStatus.Error:
        extra_msg = f"error is: {received_input}"
        logger.error("the request validation was failed", extra={"extra": extra_msg})
        raise HTTPException(status_code=400, detail=received_input)

    extra_msg = f"received_input is: {received_input}"
    logger.info(f"got a request to count the number of appearances for each word in the input",
                extra={"extra": extra_msg})
    try:
        retrieved_data = words_counter_helper.extract_text_from_input(received_input)
        if not retrieved_data:
            raise ValueError

    except ValueError as ex:
        extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
        logger.critical(f"a value error occurred while trying to extract text from input", extra={"extra": extra_msg})
        raise HTTPException(status_code=400, detail=str(ex))

    except HTTPException as ex:
        extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
        logger.critical(f"an http exception occurred while trying to extract text from input", extra={"extra": extra_msg})
        raise ex

    except Exception as ex:
        extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
        logger.critical(f"an unexpected error occurred while trying to extract text from input", extra={"extra": extra_msg})
        raise HTTPException(status_code=500, detail=str(ex))

    words = words_counter_helper.prepare_words_for_counting(retrieved_data)
    num_workers = 5
    chunk_size = len(words) // num_workers
    if not chunk_size:
        chunk_size = 1
    chunks = [words[i:i+chunk_size] for i in range(0, len(words), chunk_size)]
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for chunk in chunks:
            executor.submit(words_counter_helper.update_words_counter_mapping, chunk)
    words_counter_mapping = words_counter_helper.words_counter_mapping
    try:
        database_helper.update_database(words_counter_mapping)
        status = ResponseStatus.Ok
    except Exception as ex:
        extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
        logger.critical(f"an error occurred while trying to update database", extra={"extra": extra_msg})
        status = ResponseStatus.Error
    finally:
        words_counter_helper.clean_words_counter_mapping()
    return status


@app.get('/word_statistics/{word}')
def word_statistics(word: str) -> int:
    extra_msg = f"word is: {word}"
    logger.info("got a request to get word statistics", extra={"extra": extra_msg})
    try:
        count = database_helper.get_count_from_db(word)
    except Exception as ex:
        extra_msg = f"word is: {word}, the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
        logger.critical(f"an error occurred while trying to get word count from database", extra={"extra": extra_msg})
        raise HTTPException(status_code=500)
    return count


def main():
    database_helper.verify_db()
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    logger = create_logger()
    words_counter_helper = WordsCounterHelper(logger)
    database_helper = DatabaseHelper()
    main()
