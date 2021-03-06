
import json
import logging

from tornado.gen import coroutine

from delivery.handlers import *
from delivery.handlers.utility_handlers import ArteriaDeliveryBaseHandler

log = logging.getLogger(__name__)

class DeliverByStageIdHandler(ArteriaDeliveryBaseHandler):
    """
    Handler for starting deliveries based on a previously staged directory/file
    # TODO This is still work in progress
    """

    def initialize(self, **kwargs):
        self.delivery_service = kwargs["delivery_service"]
        super(DeliverByStageIdHandler, self).initialize(kwargs)

    @coroutine
    def post(self, staging_id):
        request_data = self.body_as_object(required_members=["delivery_project_id"])
        delivery_project_id = request_data["delivery_project_id"]

        md5sum_file = request_data.get("md5sums_file")

        # This should only be used for testing purposes /JD 20170202
        skip_mover_request = request_data.get("skip_mover")
        if skip_mover_request and skip_mover_request == True:
            log.info("Got the command to skip Mover...")
            skip_mover = True
        else:
            log.debug("Will not skip running mover!")
            skip_mover = False

        delivery_id = yield self.delivery_service.deliver_by_staging_id(staging_id=staging_id,
                                                                        delivery_project=delivery_project_id,
                                                                        md5sum_file=md5sum_file,
                                                                        skip_mover=skip_mover)

        status_end_point = "{0}://{1}{2}".format(self.request.protocol,
                                                 self.request.host,
                                                 self.reverse_url("delivery_status", delivery_id))

        self.set_status(ACCEPTED)
        self.write_json({'delivery_order_id': delivery_id,
                         'delivery_order_link': status_end_point})


class DeliveryStatusHandler(ArteriaDeliveryBaseHandler):

    def initialize(self, **kwargs):
        self.delivery_service = kwargs["delivery_service"]
        super(DeliveryStatusHandler, self).initialize(kwargs)

    @coroutine
    def get(self, delivery_order_id):
        delivery_order = yield self.delivery_service.update_delivery_status(delivery_order_id)

        self.write_json({'id': delivery_order.id,
                         'status': delivery_order.delivery_status.name,
                         'mover_delivery_id': delivery_order.mover_delivery_id})
        self.set_status(OK)
