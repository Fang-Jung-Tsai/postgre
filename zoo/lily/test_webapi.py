import unittest
import requests_mock
from webapi import webapi

class TestWebAPI(unittest.TestCase):
    def setUp(self):

        # Government Open Data Platform
        userkey     = 'CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE'
        urlprefix   = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/'

        dataid      = 'F-C0032-001'
        self.url    = f'''{urlprefix}{dataid}?Authorization={userkey}&format=JSON&sort=time'''

        self.api = webapi(self.url)
        pass

    @requests_mock.Mocker()
    def test_json_response(self, m):
        json_content = {"key": "value"}
        m.get(self.url, json=json_content)
        self.api.__init__(self.url)
        self.assertEqual(self.api.json, json_content)

    @requests_mock.Mocker()
    def test_binary_response(self, m):
        binary_content = b'{"key": "value"}'
        m.get(self.url, content=binary_content)
        self.api.__init__(self.url)
        self.assertEqual(self.api.json, {"key": "value"})

    @requests_mock.Mocker()
    def test_other_response(self, m):
        m.get(self.url, text='Not a JSON or binary response')
        self.api.__init__(self.url)
        self.assertIsNone(self.api.json)

if __name__ == '__main__':
    unittest.main()