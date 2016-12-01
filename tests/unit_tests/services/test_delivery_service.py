import unittest
from mock import MagicMock

from delivery.services.delivery_service import MoverDeliveryService
from delivery.models.db_models import DeliveryOrder, StagingOrder, StagingStatus, DeliveryStatus
from delivery.exceptions import InvalidStatusException

from tests.test_utils import MockIOLoop, MockExternalRunnerService, assert_eventually_equals


class TestMoverDeliveryService(unittest.TestCase):

    def setUp(self):

        self.mock_external_program_service = MockExternalRunnerService(return_status=0)
        self.mock_staging_service = MagicMock()
        self.mock_delivery_repo = MagicMock()

        self.delivery_order = DeliveryOrder(id=1)

        self.mock_delivery_repo.create_delivery_order.return_value = self.delivery_order
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = self.delivery_order

        self.mock_session_factory = MagicMock()
        self.mover_delivery_service = MoverDeliveryService(external_program_service=self.mock_external_program_service,
                                                           staging_service=self.mock_staging_service,
                                                           delivery_repo=self.mock_delivery_repo,
                                                           session_factory=self.mock_session_factory)
        self.mover_delivery_service.io_loop_factory = MockIOLoop

    def test_deliver_by_staging_id(self):

        staging_order = StagingOrder(source='/foo/bar', staging_target='/staging/dir/bar')
        staging_order.status = StagingStatus.staging_successful
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        self.mock_staging_service.get_delivery_order_by_id.return_value = self.delivery_order

        self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                          delivery_project='xyz123',
                                                          md5sum_file='md5sum_file')

        def _get_delivery_order():
            return self.delivery_order.delivery_status
        assert_eventually_equals(self, 1, _get_delivery_order, DeliveryStatus.delivery_in_progress)
        # TODO Finish checking this up!

    def test_deliver_by_staging_id_raises_on_non_existent_stage_id(self):
        self.mock_staging_service.get_stage_order_by_id.return_value = None

        with self.assertRaises(InvalidStatusException):

            self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                              delivery_project='foo',
                                                              md5sum_file='md5sum_file')


    def test_deliver_by_staging_id_raises_on_non_successful_stage_id(self):

        staging_order = StagingOrder()
        staging_order.status = StagingStatus.staging_failed
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        with self.assertRaises(InvalidStatusException):

            self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                              delivery_project='foo',
                                                              md5sum_file='md5sum_file')

    def test_get_status_of_delivery_order(self):
        delivery_order = DeliveryOrder(id=1,
                                       delivery_source='src',
                                       delivery_project='xyz123',
                                       delivery_status=DeliveryStatus.mover_processing_delivery,
                                       staging_order_id=11,
                                       md5sum_file='file')
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = delivery_order
        actual = self.mover_delivery_service.get_status_of_delivery_order(1)
        self.assertEqual(actual, DeliveryStatus.mover_processing_delivery)

    def test_delivery_order_by_id(self):
        delivery_order = DeliveryOrder(id=1,
                                       delivery_source='src',
                                       delivery_project='xyz123',
                                       delivery_status=DeliveryStatus.mover_processing_delivery,
                                       staging_order_id=11,
                                       md5sum_file='file')
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = delivery_order
        actual = self.mover_delivery_service.get_delivery_order_by_id(1)
        self.assertEqual(actual.id, 1)
