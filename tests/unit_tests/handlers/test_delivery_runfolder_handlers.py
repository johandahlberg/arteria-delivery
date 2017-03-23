
import json
from mock import MagicMock

from tornado.testing import *
from tornado.web import Application

from delivery.app import routes

from tests.test_utils import DummyConfig, FAKE_RUNFOLDERS


class TestRunfolderHandlers(AsyncHTTPTestCase):

    API_BASE = "/api/1.0"

    mock_runfolder_repo = MagicMock()

    def get_app(self):
        return Application(
            routes(
                config=DummyConfig(),
                runfolder_repo=self.mock_runfolder_repo))

    def test_get_runfolders(self):

        self.mock_runfolder_repo.get_runfolders.return_value = FAKE_RUNFOLDERS

        response = self.fetch(self.API_BASE + "/runfolders")

        expected_result = list([runfolder.__dict__ for runfolder in FAKE_RUNFOLDERS])
        expected_json = json.dumps({"runfolders": expected_result}, default=lambda x: x.__dict__)

        self.assertEqual(response.code, 200)
        self.assertDictEqual(json.loads(response.body), json.loads(expected_json))

    def test_get_runfolders_empty(self):

        self.mock_runfolder_repo.get_runfolders.return_value = []

        response = self.fetch(self.API_BASE + "/runfolders")

        expected_result = {"runfolders": []}

        self.assertEqual(response.code, 200)
        self.assertDictEqual(json.loads(response.body), expected_result)
