
import json


from delivery.handlers import *
from delivery.handlers.utility_handlers import ArteriaDeliveryBaseHandler


class DeliverByStageIdHandler(ArteriaDeliveryBaseHandler):
    """
    Handler for starting deliveries based on a previously staged directory/file
    # TODO This is still work in progress
    """

    def initialize(self, **kwargs):
        self.delivery_service = kwargs["delivery_service"]
        super(DeliverByStageIdHandler, self).initialize(kwargs)

    def post(self, staging_id):
        request_data = self.body_as_object(required_members=["delivery_project_id"])
        delivery_project_id = request_data["delivery_project_id"]


        md5sum_file = request_data.get("md5sums_file")

        delivery_id = self.delivery_service.deliver_by_staging_id(staging_id=staging_id,
                                                                  delivery_project=delivery_project_id,
                                                                  md5sum_file=md5sum_file)

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

    def get(self, delivery_order_id):
        delivery_order = self.delivery_service.update_delivery_status(delivery_order_id)

        self.write_json({'id': delivery_order.id,
                         'status': delivery_order.delivery_status.name,
                         'mover_delivery_id': delivery_order.mover_delivery_id})
        self.set_status(OK)
