
import json

from arteria.web.handlers import BaseRestHandler

from delivery import __version__ as version


class ArteriaDeliveryBaseHandler(BaseRestHandler):
    """
    Base handler for Arteria delivery handlers.
    """

    def initialize(self, config, **kwargs):
        """
        Ensures that any parameters feed to this are available
        to subclasses.

        :param: config configuration used by the service
        """
        self.config = config

    def write_list_of_models_as_json(self, model_list, key):
        if model_list:
            as_json = json.dumps({key: model_list}, default=lambda x: x.__dict__)
            self.write_json(as_json)
        else:
            self.write_json({key: list()})


class VersionHandler(ArteriaDeliveryBaseHandler):

    """
    Get the version of the service
    """

    def get(self):
        """
        Returns the version of the checksum-service as json. Format looks as follows:
        {
           "version": "1.0.0"
        }
        """
        self.write_object({"version": version})
