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


class RedshiftCollector(DataCollector):

    def __init__(self, connection):
        self._rs_connection = connection

    def read_total_count(self) -> int:
        rs_cursor = self._rs_connection.cursor()
        rs_cursor.execute(f"""SELECT COUNT(*) FROM temp_table""")
        return rs_cursor.fetchone()[0]

    def read_next_chunk(self, size, offset) -> Iterable:
        rs_cursor = self._rs_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        rs_cursor.execute(f"""SELECT * FROM temp_table ORDER BY ID LIMIT {size} OFFSET {offset}""")
        return rs_cursor.fetchall()


def build_de(data):
    return {'ID': data['id'],
            'EMAIL_URL': data['email']}


SalesforceConnector(client=client,
                    de_name='DATA_EXTENSION_NAME',
                    de_external_key='F21FC19B-0000-0000-0000-6C43278BF4D2',
                    data_collector=RedshiftCollector(connection), threads_size=20).stream_data_extension_using_async(build_de=build_de, offset=0, rate=500)
