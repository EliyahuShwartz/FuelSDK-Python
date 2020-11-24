import time
from collections import Iterable

import FuelSDK
from FuelSDK.data_extension_upsert import SalesforceConnector, DataCollector

client = FuelSDK.ET_Client(params={
    'clientid': '',
    'clientsecret': '',
    'defaultwsdl': "",
    'authenticationurl': "",
    'baseapiurl': "",
    'soapendpoint': "",
    'useOAuth2Authentication': '',
    'accountid': 0
})


class RedshiftCollector:

    def __init__(self, connection):
        self._rs_connection = connection

    def iterator(self, rate=1000, offset=0, limit=None):
        retry_count = 0
        fetched_rows = 0

        while (limit is None or fetched_rows + offset < limit) and retry_count < 50:
            try:
                rs_cursor = self._rs_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                optimize_limit = min(rate, limit - fetched_rows) if limit else 'ALL'
                rs_cursor.execute(f"""SELECT * FROM temp_table ORDER BY CUSTOMER_ID LIMIT {optimize_limit} OFFSET {offset}""")
                rows = rs_cursor.fetchall()
                fetched_rows += len(rows)

                if len(rows) == 0:
                    raise StopIteration()

                data_extension = dict(items=[])

                for row in rows:
                    data_extension['items'].append(self._build_data_extension(row))

                yield data_extension
            except Exception as e:
                retry_count += 1
                time.sleep(5)

    def _build_data_extension(self, row):
        return {'ID': row['id'],
                'EMAIL_URL': row['email']}


SalesforceConnector(client=client,
                    de_name='DATA_EXTENSION_NAME',
                    de_external_key='F21FC19B-0000-0000-0000-6C43278BF4D2', threads_size=20).stream_data_extension(RedshiftCollector().iterator(rate=1000, limit=2000))
