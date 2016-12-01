
import json

from delivery.models.db_models import DeliveryOrder
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

        # TODO This is just a sketch that is not yet tested!

        request_data = self.body_as_object(["delivery_project_id"])
        delivery_project_id = request_data.get("delivery_project_id")

        self.delivery_service.delivery_by_staging_id(staging_id=staging_id,
                                                     delivery_project=delivery_project_id)

        # TODO Extend this later once we know more about how this will work..


class DeliveryStatusHandler(ArteriaDeliveryBaseHandler):
    def initialize(self, **kwargs):
        self.delivery_service = kwargs["delivery_service"]
        super(DeliverByStageIdHandler, self).initialize(kwargs)

    def get(self, delivery_order_id):
        self.delivery_service.get_delivery_order_by_id(delivery_order_id)
        # TODO Return something sensible here!