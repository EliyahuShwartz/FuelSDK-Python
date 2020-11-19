import logging
import time
from asyncio import FIRST_COMPLETED
from collections import Iterable
from concurrent.futures._base import wait, ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor

import FuelSDK

de_logger = logging.getLogger('data extension upsert')


class DataCollector:
    def read_next_chunk(self, rows, offset) -> Iterable:
        pass

    def read_total_count(self) -> int:
        pass


class SalesforceConnector:

    def __init__(self, client, de_name, de_external_key, data_collector: DataCollector):
        self.de_external_key = de_external_key
        self.de_name = de_name
        self.client = client
        self.data_collector = data_collector

    def __update_data_extension_rows(self, kwargs):
        de_row = FuelSDK.ET_DataExtension_Rows(self.de_external_key)
        de_row.auth_stub = self.client
        de_row.Name = self.de_name
        de_row.props = kwargs

        retry_count = 0
        sent_successfully = False
        sleep_time = 4

        while sent_successfully is not True and retry_count < 3:
            try:
                de_logger.debug(f'sending request :{kwargs} for the {retry_count} time')
                results = de_row.put()
                sent_successfully = results.code in range(200, 300)
                de_logger.info(f'result content :{results.results}, status code {results.code}')

                if sent_successfully is not True:
                    retry_count += 1
                    de_logger.warning(f'sleeping for  :{sleep_time} sec')
                    time.sleep(sleep_time)
                    sleep_time *= 2

            except Exception as e:
                retry_count += 1
                de_logger.error(e)

        return sent_successfully

    def stream_data_into_data_extension(self, build_de, offset=0, rate=100):

        rows_count = self.data_collector.read_total_count()

        fetched_rows = offset
        data_extension = dict(items=[])

        retry_count = 0
        start_time = time.time()
        total_query_time = 0
        max_pending = 60
        futures = set()

        with ThreadPoolExecutor(max_workers=60) as pool:
            while fetched_rows < rows_count:
                try:
                    query_time = time.time()

                    next_chunk = self.data_collector.read_next_chunk(rate, fetched_rows)

                    query_time = time.time() - query_time
                    total_query_time += query_time

                    for item in next_chunk:
                        data_extension['items'].append(build_de(item))

                    futures.add(pool.submit(self.__update_data_extension_rows, data_extension))

                    while len(futures) > max_pending:
                        completed_tasks, futures = wait(futures, None, FIRST_COMPLETED)

                    data_extension = dict(items=[])
                    retry_count = 0
                    fetched_rows += rate

                    fetched_rows_offset = (fetched_rows - offset)
                    de_logger.info(f'Fetched rows from redshift {fetched_rows_offset} : in time {query_time}')
                    de_logger.info(f'DE send rate {fetched_rows_offset / (time.time() - start_time)} per sec')
                    de_logger.info(f'DE send rate without redshift query time {fetched_rows_offset / (time.time() - (start_time + total_query_time))} per sec')
                except Exception as e:
                    retry_count += 1
                    de_logger.warning(e)

            wait(futures, None, ALL_COMPLETED)
            de_logger.info(f'DE - total send time {time.time() - start_time}')
