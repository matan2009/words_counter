from concurrent.futures import ThreadPoolExecutor, as_completed
from json import JSONDecodeError

from fastapi import FastAPI, Request, HTTPException
import uvicorn

from monitoring.logger import create_logger
from service.database_helper import DatabaseHelper
from service.words_counter_helper import WordsCounterHelper
from type.response_status import ResponseStatus

app = FastAPI()


async def get_request_body(request):
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
            return ResponseStatus.Ok, received_input
        except JSONDecodeError:
            return 'Invalid JSON data.'
    else:
        return ResponseStatus.Error, 'Content-Type not supported.'


@app.post('/word_counter/')
async def word_counter(request: Request) -> ResponseStatus or HTTPException:
    body_extraction_status, content = await get_request_body(request)
    if body_extraction_status == ResponseStatus.Error:
        extra_msg = f"error is: {content}"
        logger.error("failed to get request body", extra={"extra": extra_msg})
        raise HTTPException(status_code=400, detail=content)
    extra_msg = f"received_input is: {content}"
    logger.info(f"got a request to count the number of appearances for each word in the input",
                extra={"extra": extra_msg})
    # Placeholder for classifying the input
    words = content.split()
    num_workers = 5
    chunk_size = len(words) // num_workers
    if not chunk_size:
        chunk_size = 1
    chunks = [words[i:i+chunk_size] for i in range(0, len(words), chunk_size)]
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for chunk in chunks:
            executor.submit(words_counter_helper.update_words_counter_mapping, chunk)
    words_counter_mapping = words_counter_helper.words_counter_mapping
    database_helper.update_database(words_counter_mapping)
    words_counter_helper.clean_words_counter_mapping()
    return ResponseStatus.Ok


def main():
    database_helper.verify_db()
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    logger = create_logger()
    words_counter_helper = WordsCounterHelper(logger)
    database_helper = DatabaseHelper()
    main()
