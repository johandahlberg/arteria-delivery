
import logging

from tornado.gen import Task, coroutine
from tornado.web import asynchronous

from arteria.web.handlers import BaseRestHandler

from delivery.handlers import *
from delivery.exceptions import ProjectNotFoundException


log = logging.getLogger(__name__)


class BaseStagingHandler(BaseRestHandler):

    def _construct_status_endpoint(self, status_id):
        status_end_point = "{0}://{1}{2}".format(self.request.protocol,
                                                 self.request.host,
                                                 self.reverse_url("stage_status", status_id))
        return status_end_point

    def _construct_response_from_project_and_status(self, staging_order_projects_and_ids):
        link_results = {}
        id_results = {}
        for project, status_id in staging_order_projects_and_ids.items():
            link_results[project] = self._construct_status_endpoint(status_id)
            id_results[project] = status_id

        return link_results, id_results


class StagingRunfolderHandler(BaseStagingHandler):
    """
    Handler class for handling how to start staging of a runfolder. Polling for status, canceling, etc can then be
    handled by the more general `StagingHandler`
    """

    def initialize(self, staging_service, **kwargs):
        self.staging_service = staging_service

    @coroutine
    def post(self, runfolder_id):
        """
        Attempt to stage projects from the the specified runfolder, so that they can then be delivered.
        Will return a set of status links, one for each project that can be queried for the status of
        that staging attempt. A list of project names can be specified in the request body to limit which projects
        should be staged. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/stage/runfolder/160930_ST-E00216_0111_BH37CWALXX"

            payload = "{'projects': ['ABC_123']}"
            headers = {
                'content-type': "application/json",
            }

            response = requests.request("POST", url, data=payload, headers=headers)

            print(response.text)

        The return format looks like:
            {"staging_order_links": {"ABC_123": "http://localhost:8080/api/1.0/stage/584"}}

        """

        log.debug("Trying to stage runfolder with id: {}".format(runfolder_id))

        try:
            request_data = self.body_as_object()
        except ValueError:
            request_data = {}

        try:
            projects_to_stage = request_data.get("projects", [])

            log.debug("Got the following projects to stage: {}".format(projects_to_stage))

            staging_order_projects_and_ids = self.staging_service.stage_runfolder(runfolder_id, projects_to_stage)

            link_results, id_results = self._construct_response_from_project_and_status(staging_order_projects_and_ids)

            self.set_status(ACCEPTED)
            self.write_json({'staging_order_links': link_results,
                             'staging_order_ids': id_results})
        except ProjectNotFoundException as e:
            self.set_status(NOT_FOUND, reason=e.msg)


class StageGeneralDirectoryHandler(BaseStagingHandler):
    """
    Handler used to stage projects which are represented as directories in a root directory specified by
    `general_project_directory` in the application config.
    """

    def initialize(self, staging_service, **kwargs):
        self.staging_service = staging_service

    def post(self, directory_name):
        """
        Attempt to stage projects (represented by directories under a configurable root directory),
        so that they can then be delivered.
        Will return a set of status links, one for each project that can be queried for the status of
        that staging attempt. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/stage/project/my_test_project"

            headers = {
                'content-type': "application/json",
            }

            response = requests.request("POST", url, data='', headers=headers)

            print(response.text)

        The return format looks like:
            {"staging_order_links": {"my_test_project": "http://localhost:8080/api/1.0/stage/591"}}

        """
        stage_order_and_id = self.staging_service.stage_directory(directory_name)

        link_results, id_results = self._construct_response_from_project_and_status(stage_order_and_id)

        self.set_status(ACCEPTED)
        self.write_json({'staging_order_links': link_results,
                         'staging_order_ids': id_results})

class StagingHandler(BaseRestHandler):

    def initialize(self, staging_service, **kwargs):
        self.staging_service = staging_service

    def get(self, stage_id):
        """
        Returns the current status as json of the of the staging order, or 404 if the order is unknown.
        Possible values for status are: pending, staging_in_progress, staging_successful, staging_failed
        Return format looks like:
        {
           "status": "staging_successful"
        }
        """
        stage_order = self.staging_service.get_stage_order_by_id(stage_id)
        if stage_order:
            self.write_json({'status': stage_order.status.name, 'size': stage_order.size})
        else:
            self.set_status(NOT_FOUND, reason='No stage order with id: {} found.'.format(stage_id))

    def delete(self, stage_id):
        """
        Kill a stage order with the give id. Will return status 204 if the staging process was successfully cancelled,
        otherwise it will return status 500.
        """
        was_killed = self.staging_service.kill_process_of_stage_order(stage_id)
        if was_killed:
            self.set_status(NO_CONTENT)
        else:
            self.set_status(INTERNAL_SERVER_ERROR,
                            reason="Could not kill stage order with id: {}, either it wasn't in a state "
                                   "which allows it to be killed, or the pid associated with the stage order "
                                   "did not allow itself to be killed. Consult the server logs for an exact "
                                   "reason.")
