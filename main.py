from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI
import uvicorn

from monitoring.logger import create_logger
from service.database_helper import DatabaseHelper
from service.words_counter_helper import WordsCounterHelper
from type.response_status import ResponseStatus

app = FastAPI()


@app.post('/word_counter/{received_input}')
async def word_counter(received_input: str) -> ResponseStatus:
    extra_msg = f"received_input is: {received_input}"
    logger.info(f"got a request to count the number of appearances for each word in the input",
                extra={"extra": extra_msg})
    # Placeholder for classifying the input
    words = received_input.split()
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
