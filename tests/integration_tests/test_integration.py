

import json
from functools import partial


from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.models.db_models import StagingStatus, DeliveryStatus

from tests.test_utils import assert_eventually_equals


class TestIntegration(AsyncHTTPTestCase):

    def _get_delivery_status(self, link):
        self.http_client.fetch(link, self.stop)
        status_response = self.wait()
        return json.loads(status_response.body)["status"]

    def _get_size(self, staging_link):
        self.http_client.fetch(staging_link, self.stop)
        status_response = self.wait()
        return json.loads(status_response.body)["size"]


    API_BASE = "/api/1.0"

    def get_app(self):

        # Get an as similar app as possible, tough note that we don't use the
        #  app service start method to start up the the application
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

        runfolder_names = []
        for runfolder_json in response_json["runfolders"]:
            runfolder_names.append(runfolder_json["name"])

        self.assertIn("160930_ST-E00216_0112_AH37CWALXX", runfolder_names)

        self.assertIn("160930_ST-E00216_0111_BH37CWALXX", runfolder_names)

    def test_can_return_projects(self):
        response = self.fetch(self.API_BASE + "/projects")
        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        first_project = response_json["projects"][0]
        self.assertEqual(first_project["name"], "ABC_123")

    def test_can_stage_and_delivery_runfolder(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        url = "/".join([self.API_BASE, "stage", "runfolder", "160930_ST-E00216_0111_BH37CWALXX"])
        response = self.fetch(url, method='POST', body='')
        self.assertEqual(response.code, 202)

        response_json = json.loads(response.body)

        staging_status_links = response_json.get("staging_order_links")

        for project, link in staging_status_links.items():

            self.assertEqual(project, "ABC_123")

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_delivery_status, link),
                                     expected=StagingStatus.staging_successful.name)

            # The size of the fake project is 15 bytes
            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_size, link),
                                     expected=15)

        staging_order_project_and_id = response_json.get("staging_order_ids")

        for project, staging_id in staging_order_project_and_id.items():
            delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
            delivery_body = {'delivery_project_id': 'fakedeliveryid2016',
                             'skip_mover': True}
            delivery_resp = self.fetch(delivery_url, method='POST', body=json.dumps(delivery_body))
            delivery_resp_as_json = json.loads(delivery_resp.body)
            delivery_link = delivery_resp_as_json['delivery_order_link']

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_delivery_status, delivery_link),
                                     expected=DeliveryStatus.delivery_skipped.name)

    def test_can_stage_and_delivery_project_dir(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        url = "/".join([self.API_BASE, "stage", "project", "my_test_project"])
        response = self.fetch(url, method='POST', body='')
        self.assertEqual(response.code, 202)

        response_json = json.loads(response.body)

        staging_status_links = response_json.get("staging_order_links")

        for project, link in staging_status_links.items():
            self.assertEqual(project, "my_test_project")

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_delivery_status, link),
                                     expected=StagingStatus.staging_successful.name)

        staging_order_project_and_id = response_json.get("staging_order_ids")

        for project, staging_id in staging_order_project_and_id.items():
            delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
            delivery_body = {'delivery_project_id': 'fakedeliveryid2016',
                             'skip_mover': True}
            delivery_resp = self.fetch(delivery_url, method='POST', body=json.dumps(delivery_body))
            delivery_resp_as_json = json.loads(delivery_resp.body)
            delivery_link = delivery_resp_as_json['delivery_order_link']

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_delivery_status, delivery_link),
                                     expected=DeliveryStatus.delivery_skipped.name)

    def test_can_stage_and_deliver_clean_flowcells(self):
        self.assertTrue(False)

    def test_can_stage_and_deliver_batched_flowcells(self):
        self.assertTrue(False)

    def test_can_stage_and_deliver_force_flowcells(self):
        self.assertTrue(False)
