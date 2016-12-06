

import json
import os
from mock import MagicMock


from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.models.db_models import StagingStatus

from tests.test_utils import assert_eventually_equals


class TestIntegration(AsyncHTTPTestCase):

    API_BASE = "/api/1.0"

    def get_app(self):

        # Get an as similar app as possible, tough note that we don't use the
        #  app service start method to start up the the application
        # TODO
        # Also not that that we need to mock away the delivery service, since Mover is
        # not expected to be installed on the developers system.
        path_to_this_file = os.path.abspath(
            os.path.dirname(os.path.realpath(__file__)))
        app_svc = AppService.create(product_name="test_delivery_service",
                                    config_root="{}/../../config/".format(path_to_this_file))

        config = app_svc.config_svc

        composed_application = compose_application(config)
        # TODO Later swap the "real" delivery service here for mock one.

        return Application(app_routes(**composed_application))

    def test_can_return_flowcells(self):
        response = self.fetch(self.API_BASE + "/runfolders")

        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        first_runfolder = response_json["runfolders"][0]
        self.assertEqual(first_runfolder["name"], "160930_ST-E00216_0111_BH37CWALXX")

    def test_can_return_projects(self):
        response = self.fetch(self.API_BASE + "/projects")
        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        first_project = response_json["projects"][0]
        self.assertEqual(first_project["name"], "ABC_123")

    def test_can_stage_delivery(self):
        url = "/".join([self.API_BASE, "stage", "runfolder", "160930_ST-E00216_0111_BH37CWALXX"])
        response = self.fetch(url, method='POST', body='')
        self.assertEqual(response.code, 202)

        response_json = json.loads(response.body)

        staging_status_links = response_json.get("staging_order_links")

        for project, link in staging_status_links.iteritems():

            self.assertEqual(project, "ABC_123")

            def _get_delivery_status():
                self.http_client.fetch(link, self.stop)
                status_response = self.wait()
                return json.loads(status_response.body)["status"]

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=_get_delivery_status,
                                     expected=StagingStatus.staging_successful.name)

    def test_can_deliver_data(self):
        pass
