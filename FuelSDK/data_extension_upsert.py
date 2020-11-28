import copy
import logging
import time
from asyncio import FIRST_COMPLETED
from concurrent.futures._base import wait, ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Generator

import FuelSDK

de_logger = logging.getLogger('DataExtension-Rest: ')


class SalesforceConnector:

    def __init__(self, client, de_name, de_external_key, threads_size=50, sending_retry=5, chunk_size=3000):
        self.de_external_key = de_external_key
        self.de_name = de_name
        self.client = client
        self.threads_size = threads_size
        self.sending_retry = sending_retry
        self.chunk_size = chunk_size

    def _update_data_extension_rows(self, de, iterator_range):

        de_row = FuelSDK.ET_DataExtension_Rows(self.de_external_key)
        de_row.auth_stub = self.client
        de_row.Name = self.de_name
        de_row.props = de
        sent_successfully = False

        retry_count = 0
        sleep_time = 4

        de_logger.debug(f'Sending request range :{iterator_range} for the {retry_count} time')

        while not sent_successfully and retry_count < self.sending_retry:

            try:
                if isinstance(de, list):
                    results = de_row.post()
                else:
                    results = de_row.put()

                sent_successfully = results.code in range(200, 300)

                de_logger.debug(f'Result : {results.results}')

                if not sent_successfully:
                    de_logger.warning(f'request were failed with status code {results.code}, sleeping for :{sleep_time} sec')
                    time.sleep(sleep_time)
                    sleep_time *= 2
                else:
                    de_logger.info(f'{iterator_range} DE sent successfully')
                    return iterator_range, results.results
            except Exception as e:
                de_logger.debug(e)
            finally:
                retry_count += 1

        de_logger.error(f'Request :{de} failed to sent after {retry_count} retries')
        return iterator_range, None

    def chunk_grouper(self, generator):
        group = []
        for item in generator:
            group.append(item)
            if len(group) == self.chunk_size:
                yield group
                group = []
        yield group

    def stream_data_extension(self, generator: Generator) -> dict:
        """
        :param generator: generator of data extension
        :return: result of dict of query range with the request id in format of : {'10-100': %RequestId}
        """
        start_time = time.time()
        fetched_rows = 0
        futures = set()
        all_completed_tasks = set()

        with ThreadPoolExecutor(max_workers=self.threads_size) as pool:
            for chunk in self.chunk_grouper(generator):
                try:
                    de_length = len(chunk['items']) if isinstance(chunk, dict) else len(chunk)
                    iterator_range = f'{fetched_rows}-{fetched_rows + de_length}'
                    futures.add(pool.submit(self._update_data_extension_rows, de=copy.deepcopy(chunk), iterator_range=iterator_range))

                    while len(futures) > self.threads_size:
                        completed_tasks, futures = wait(futures, None, FIRST_COMPLETED)
                        all_completed_tasks.update(completed_tasks)

                    fetched_rows += de_length
                    de_logger.info(f'{iterator_range} DE is send to salesforce')
                    de_logger.info(f'send rate {fetched_rows / (time.time() - start_time)} per sec')
                except Exception as e:
                    de_logger.warning(f'Failed to read/submit data extension: {e}')

        de_logger.info(f'DE - Waiting for {len(futures)} jobs to complete...')
        completed_tasks, futures = wait(futures, None, ALL_COMPLETED)
        all_completed_tasks.update(completed_tasks)

        de_logger.info(f'Send rate {fetched_rows / (time.time() - start_time)} per sec')
        de_logger.info(f'DE - fetched_rows sent time: {time.time() - start_time}')

        results = dict()

        for task in all_completed_tasks:
            query_range, result = task.result()
            results[query_range] = result

        return results

    def verify_async_requests(self, requests_ids: list) -> dict:
        """
        :param requests_ids: list of request ids
        :return: dict of requests ids and their result (boolean)
        """
        futures = set()
        all_completed_tasks = set()
        max_pending = self.threads_size

        with ThreadPoolExecutor(max_workers=self.threads_size) as pool_v2:

            for request_id in requests_ids:
                futures.add(pool_v2.submit(self.verify_request, request_id['requestId']))

                while len(futures) > max_pending:
                    completed_tasks, futures = wait(futures, None, FIRST_COMPLETED)
                    all_completed_tasks.update(completed_tasks)

        completed_tasks, futures = wait(futures, None, ALL_COMPLETED)
        all_completed_tasks.update(completed_tasks)

        results = dict()

        for index, task in enumerate(all_completed_tasks):
            results[requests_ids[index]['requestId']] = task.result()

        return results

    def verify_request(self, request_id: str):
        """
        :param request_id: request id
        :return:boolean of success or failure
        """
        de_row = FuelSDK.ET_Async_StatusResult()
        de_row.auth_stub = self.client
        result_fetching_sleep_time = 4
        retry_count = 0

        while retry_count < 3:
            r = de_row.get_status(request_id)
            try:
                if r.code in range(200, 300) and "status" in r.results and "requestStatus" in r.results["status"] and r.results["status"]["requestStatus"] == 'Complete':
                    de_logger.info(f'request was verified {request_id}, result status {r.results["status"]["resultStatus"]}')

                    if r.results["status"]["hasErrors"]:
                        de_logger.warning(r.results)
                        return False
                    return True
                else:
                    de_logger.debug(r.results)
                    de_logger.debug(f'sleeping for :{result_fetching_sleep_time} sec to get result of request {request_id}')
                    time.sleep(result_fetching_sleep_time)
            except Exception as e:
                de_logger.warning(e)
                time.sleep(result_fetching_sleep_time)
            finally:
                retry_count += 1
                result_fetching_sleep_time *= 2

        return False
