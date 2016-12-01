import unittest
from mock import MagicMock

from delivery.services.delivery_service import MoverDeliveryService
from delivery.models.db_models import DeliveryOrder, StagingOrder, StagingStatus
from delivery.models.runfolder import Runfolder
from delivery.exceptions import InvalidStatusException


class TestMoverDeliveryService(unittest.TestCase):

    def setUp(self):

        self.mock_external_program_service = MagicMock()
        self.mock_staging_service = MagicMock()
        self.mock_delivery_repo = MagicMock()
        self.mock_session_factory = MagicMock()
        self.mover_delivery_service = MoverDeliveryService(external_program_service=self.mock_external_program_service,
                                                           staging_service=self.mock_staging_service,
                                                           delivery_repo=self.mock_delivery_repo,
                                                           session_factory=self.mock_session_factory)

    def test_deliver_by_staging_id(self):
        # TODO
        pass

    def test_deliver_by_staging_id_raises_on_non_existent_stage_id(self):
        self.mock_staging_service.get_stage_order_by_id.return_value = None

        with self.assertRaises(InvalidStatusException):

            self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                              delivery_project='foo')

    def test_deliver_by_staging_id_raises_on_non_successful_stage_id(self):

        staging_order = StagingOrder()
        staging_order.status = StagingStatus.staging_failed
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        with self.assertRaises(InvalidStatusException):

            self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                              delivery_project='foo')

    def test_get_status_of_delivery_order(self):
        # TODO
        pass
