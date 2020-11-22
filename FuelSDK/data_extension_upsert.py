import logging
import sys
import time
from asyncio import FIRST_COMPLETED
from collections import Iterable
from concurrent.futures._base import wait, ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor

import FuelSDK

de_logger = logging.getLogger('data extension - Rest: ')


class DataCollector:
    def read_next_chunk(self, rows, offset) -> Iterable:
        """
        :param rows: the number of rows to read in each chunk
        :param offset: the offset of rows (that already read)
        :return: chunk of data
        """
        pass

    def read_total_count(self) -> int:
        """
        :return: count of total rows
        """
        pass


class DERest:

    def __init__(self, build_de):
        self.build_de = build_de

    def append(self, data):
        pass

    def get(self):
        pass

    def clear(self):
        pass


class DESync(DERest):

    def __init__(self, build_de):
        super().__init__(build_de)
        self.de = []

    def append(self, data):
        self.de.append(self.build_de(data))

    def clear(self):
        self.de = []

    def get(self):
        return self.de


class DEAsync(DERest):

    def __init__(self, build_de):
        super().__init__(build_de)
        self.de = dict(items=[])

    def append(self, data):
        self.de['items'].append(self.build_de(data))

    def clear(self):
        self.de = dict(items=[])

    def get(self):
        return self.de


class SalesforceConnector:

    def __init__(self, client, de_name, de_external_key, data_collector: DataCollector, threads_size=50, sending_retry=5):
        self.de_external_key = de_external_key
        self.de_name = de_name
        self.client = client
        self.data_collector = data_collector
        self.threads_size = threads_size
        self.sending_retry = sending_retry

    def _update_data_extension_rows(self, de):

        de_row = FuelSDK.ET_DataExtension_Rows(self.de_external_key)
        de_row.auth_stub = self.client
        de_row.Name = self.de_name
        de_row.props = de.get()
        sent_successfully = False

        try:
            retry_count = self.sending_retry
            sleep_time = 4

            de_logger.debug(f'sending request :{de.get()} for the {retry_count} time, approximate size: {sys.getsizeof(de.get())}')

            while not sent_successfully and retry_count < 5:

                if isinstance(de, DESync):
                    results = de_row.post()
                else:
                    results = de_row.put()

                sent_successfully = results.code in range(200, 300)

                de_logger.debug(f'Result : {results.results}')

                if not sent_successfully:
                    retry_count += 1
                    de_logger.warning(f'request were failed with status code {results.code}, sleeping for :{sleep_time} sec')
                    time.sleep(sleep_time)
                    sleep_time *= 2
                else:
                    return results.results

        except Exception as e:
            de_logger.error(e)

    def stream_data_extension_using_async(self, build_de, offset=0, rate=100) -> dict:
        return self.__stream_data_extension(DEAsync(build_de), offset, rate)

    def stream_data_extension_using_sync(self, build_de, offset=0, rate=100) -> dict:
        return self.__stream_data_extension(DESync(build_de), offset, rate)

    def __stream_data_extension(self, data_extension: DERest, offset=0, rate=100) -> dict:
        rows_count = self.data_collector.read_total_count()

        fetched_rows = offset
        start_time = time.time()
        total_query_time = 0
        max_pending = self.threads_size
        futures = set()
        all_completed_tasks = set()

        with ThreadPoolExecutor(max_workers=self.threads_size) as pool:
            while fetched_rows < rows_count:
                try:
                    query_time = time.time()

                    next_chunk = self.data_collector.read_next_chunk(rate, fetched_rows)

                    query_time = time.time() - query_time
                    total_query_time += query_time

                    for item in next_chunk:
                        data_extension.append(item)

                    futures.add(pool.submit(self._update_data_extension_rows, de=data_extension))

                    while len(futures) > max_pending:
                        completed_tasks, futures = wait(futures, None, FIRST_COMPLETED)
                        all_completed_tasks.update(completed_tasks)

                    data_extension.clear()
                    fetched_rows += rate

                    fetched_rows_offset = (fetched_rows - offset)
                    de_logger.info(f'Fetched rows from data collector {fetched_rows_offset} : in time {query_time}')
                    de_logger.info(f'DE send rate {fetched_rows_offset / (time.time() - start_time)} per sec')
                    de_logger.info(f'DE send rate without data collector query time {fetched_rows_offset / (time.time() - (start_time + total_query_time))} per sec')

                except Exception as e:
                    de_logger.warning(f'Failed to read/submit data extension: {e}')

            de_logger.info(f'DE - Waiting for all jobs to complete {len(futures)}')
            completed_tasks, futures = wait(futures, None, ALL_COMPLETED)
            all_completed_tasks.update(completed_tasks)

        de_logger.info(f'DE send rate without data collector query time {fetched_rows_offset / (time.time() - (start_time + total_query_time))} per sec')
        de_logger.info(f'DE send rate {fetched_rows_offset / (time.time() - start_time)} per sec')
        de_logger.info(f'DE - total send time {time.time() - start_time}')

        results = dict()

        for index, task in enumerate(all_completed_tasks):
            results[(index * rate) + offset] = task.result()

        return results

    def verify_async_requests(self, requests_ids: list) -> dict:
        futures = set()
        all_completed_tasks = set()
        max_pending = self.threads_size

        with ThreadPoolExecutor(max_workers=self.threads_size) as pool_v2:

            for request_id in requests_ids:
                futures.add(pool_v2.submit(self.verify_request, request_id))

                while len(futures) > max_pending:
                    completed_tasks, futures = wait(futures, None, FIRST_COMPLETED)
                    all_completed_tasks.update(completed_tasks)

        completed_tasks, futures = wait(futures, None, ALL_COMPLETED)
        all_completed_tasks.update(completed_tasks)

        results = dict()

        for index, task in enumerate(all_completed_tasks):
            results[request_id[index]] = task.result()

        return results

    def verify_request(self, request_id: str):
        de_row = FuelSDK.ET_Async_StatusResult()
        de_row.auth_stub = self.client
        result_fetching_sleep_time = 4
        retry_count = 0

        while retry_count < 3:
            r = de_row.get_status(request_id)
            if r.code in range(200, 300) and "status" in r.results and "requestStatus" in r.results["status"] and r.results["status"]["requestStatus"] == 'Complete':
                de_logger.info(f'request was verified {request_id}, result status {r.results["status"]["resultStatus"]}')

                if r.results["status"]["hasErrors"]:
                    de_logger.warning(r.results)
                    return False
                return True
            else:
                de_logger.warning(r.results)
                retry_count += 1
                de_logger.warning(f'sleeping for :{result_fetching_sleep_time} sec to get result of request {request_id}')
                time.sleep(result_fetching_sleep_time)
                result_fetching_sleep_time *= 2
        return False
