
import logging

from delivery.exceptions import InvalidStatusException
from delivery.models.db_models import StagingStatus, DeliveryStatus

log = logging.getLogger(__name__)


class MoverDeliveryService(object):

    def __init__(self, external_program_service, staging_service, delivery_repo, session_factory):
        self.external_program_service = external_program_service
        self.staging_service = staging_service
        self.delivery_repo = delivery_repo
        self.session_factory = session_factory


    @staticmethod
    def _run_mover(delivery_order_id, delivery_order_repo, external_program_service, session_factory):
        session = session_factory()

        # This is a somewhat hacky work-around to the problem that objects created in one
        # thread, and thus associated with another session cannot be accessed by another
        # thread, there fore it is re-materialized in here...
        delivery_order = delivery_order_repo.get_delivery_order_by_id(delivery_order_id, session)
        try:

            cmd = ['mover',
                   'deliver',
                   delivery_order.delivery_source,
                   delivery_order.delivery_project,
                   delivery_order.md5sum_file]

            execution = external_program_service.run(cmd)
            delivery_order.delivery_status = DeliveryStatus.mover_processing_delivery
            delivery_order.mover_pid = execution.pid
            session.commit()

            execution_result = external_program_service.wait_for_execution(execution)

            if execution_result.status_code == 0:
                delivery_order.delivery_status = DeliveryStatus.delivery_in_progress
                log.info("Successfully started delivery of with Mover: {}".format(delivery_order))
            else:
                delivery_order.delivery_status = DeliveryStatus.delivery_failed
                log.info("Failed to start Mover delivery: {}. Mover returned status code: {}".
                         format(delivery_order, execution_result.status_code))

        # TODO Better exception handling here...
        except Exception as e:
            delivery_order.delivery_status = DeliveryStatus.delivery_failed
            log.info("Failed in starting delivery: {} because this exception was logged: {}".
                     format(delivery_order, e))
        finally:
            # Always commit the state change to the database
            session.commit()

    def deliver_by_staging_id(self, staging_id, delivery_project):

        stage_order = self.staging_service.get_stage_order_by_id(staging_id)
        if not stage_order or not stage_order.status == StagingStatus.staging_successful:
            raise InvalidStatusException("Only deliver by staging_id if it has a successful status!"
                                         "Staging order was: {}".format(stage_order))

        # TODO Adjust staging_target to fit with exactly what we want to deliver
        delivery_order = self.delivery_repo.create_delivery_order(delivery_source=stage_order.staging_target,
                                                                  delivery_project=delivery_project,
                                                                  delivery_status=DeliveryStatus.pending,
                                                                  staging_order_id=staging_id)

        #TODO Start Mover process in external thread...


    def get_status_of_delivery_order(self, delivery_order_id):
        pass

