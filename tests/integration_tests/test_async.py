
import json
from functools import partial


from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.models.db_models import StagingStatus, DeliveryStatus

from tests.test_utils import assert_eventually_equals


class TestIntegration(AsyncHTTPTestCase):


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

    def test_can_be_async(self):
        response = self.fetch(self.API_BASE + '/test')

        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(response_json['status'], 'OK')
