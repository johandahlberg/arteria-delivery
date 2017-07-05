
import logging
import os
import signal
import re

from tornado import gen

from delivery.models.db_models import StagingStatus
from delivery.exceptions import RunfolderNotFoundException, InvalidStatusException,\
    ProjectNotFoundException, TooManyProjectsFound

from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class StagingService(object):
    """
    Starting in this context means copying a directory or file to a separate directory before delivering it.
    This service handles that in a asynchronous way. Copying operations (right nwo powered by rsync) can be
    started, and their status monitored by querying the underlying database for their status.
    """

    # TODO On initiation of a Staging service, restart any ongoing stagings
    # since they should all have been killed.
    # And if we do so we need to make sure that the Staging service
    # acts as a singleton, look at:
    # http://python-3-patterns-idioms-test.readthedocs.io/en/latest/Singleton.html
    #
    # Alternative suggestion from Steinar on how to solve the problem, which is probably better:
    # "Do you mean to ensure that only one thread tries doing that at a time? An idea could
    #  be to take a database lock to ensure this, i.e. fetch all objects in the unfinished
    #  state, restart them, change the status and then commit, locking the sqlite database
    #  briefly while doing so (I think row level locking is limited in sqlite.)"
    #  / JD 20161111

    def __init__(self,
                 staging_dir,
                 external_program_service,
                 staging_repo,
                 runfolder_repo,
                 project_dir_repo,
                 project_links_directory,
                 runfolder_project_repo,
                 session_factory):
        """
        Instantiate a new StagingService
        :param staging_dir: the directory to which files/dirs should be staged
        :param external_program_service: a instance of ExternalProgramService
        :param staging_repo: a instance of DatabaseBasedStagingRepository
        :param runfolder_repo: a instance of FileSystemBasedRunfolderRepository
        :param project_dir_repo: a instance of GeneralProjectRepository
        :param project_links_directory: a path to a directory where links will be created temporarily
                                        before they are rsynced into staging (for batched deliveries etc)
        :param runfolder_project_repo: A runfolder project repo
        :param session_factory: a factory method which can produce new sqlalchemy Session instances
        """
        self.staging_dir = staging_dir
        self.external_program_service = external_program_service
        self.staging_repo = staging_repo
        self.runfolder_repo = runfolder_repo
        self.project_dir_repo = project_dir_repo
        self.project_links_directory = project_links_directory
        self.runfolder_project_repo = runfolder_project_repo
        self.session_factory = session_factory

    @staticmethod
    @gen.coroutine
    def _copy_dir(staging_order_id, external_program_service, session_factory, staging_repo):
        """
        Copies the file or directory indicated by the staging order by calling the external_program_service.
        It will attempt the copying and update the database with the status of the StagingOrder depending on the
        outcome.
        :param staging_order_id: The id of the staging order to execute
        :param external_program_service: A instance of ExternalProgramService
        :param session_factory: A factory method which can produce a new sql alchemy Session instance
        :param staging_repo: A instance of DatabaseBasedStagingRepository
        :return: None, only reports back through side-effects
        """

        session = session_factory()

        # This is a somewhat hacky work-around to the problem that objects created in one
        # thread, and thus associated with another session cannot be accessed by another
        # thread, there fore it is re-materialized in here...
        staging_order = staging_repo.get_staging_order_by_id(staging_order_id, session)
        try:

            cmd = ['rsync', '--stats', '-r', '--copy-links', staging_order.source, staging_order.staging_target]

            execution = external_program_service.run(cmd)

            staging_order.pid = execution.pid
            session.commit()

            execution_result = yield external_program_service.wait_for_execution(execution)
            log.debug("Execution result: {}".format(execution_result))
            if execution_result.status_code == 0:

                # Parse the file size from the output of rsync stats:
                # Total file size: 207,707,566 bytes
                match = re.search('Total file size: ([\d,]+) bytes',
                                  execution_result.stdout,
                                  re.MULTILINE)
                size_of_transfer = match.group(1)
                size_of_transfer = int(size_of_transfer.replace(",", ""))
                staging_order.size = size_of_transfer

                staging_order.status = StagingStatus.staging_successful
                log.info("Successfully staged: {} to: {}".format(staging_order, staging_order.get_staging_path()))
            else:
                staging_order.status = StagingStatus.staging_failed
                log.info("Failed in staging: {} because rsync returned exit code: {}".
                         format(staging_order, execution_result.status_code))

        # TODO Better exception handling here...
        except Exception as e:
            staging_order.status = StagingStatus.staging_failed
            log.info("Failed in staging: {} because this exception was logged: {}".
                     format(staging_order, e))
        finally:
            # Always commit the state change to the database
            session.commit()

    @gen.coroutine
    def stage_order(self, stage_order):
        """
        Validate a staging order and hand of the actual stating to a separate thread.
        :param stage_order: to stage
        :return: None
        """

        session = self.session_factory()

        try:

            if stage_order.status != StagingStatus.pending:
                raise InvalidStatusException("Cannot start staging a delivery order with status: {}".
                                             format(stage_order.status))

            stage_order.status = StagingStatus.staging_in_progress
            session.commit()

            args_for_copy_dir = {"staging_order_id": stage_order.id,
                                 "external_program_service": self.external_program_service,
                                 "staging_repo": self.staging_repo,
                                 "session_factory": self.session_factory}

            yield StagingService._copy_dir(**args_for_copy_dir)

        # TODO Better error handling
        except Exception as e:
            stage_order.status = StagingStatus.staging_failed
            session.commit()
            raise e

    def _create_links_area_for_project_runfolders(self, project_name, projects):
        """
        Creates a directory in which it creates links to all runfolders for the projects
        given. This is useful so that we can then rsync that directory to
        the staging area.
        :param project_name: name of the project
        :param projects: runfolders with the specified project on them
        :return: the path to the dir created
        """

        project_dir = os.path.join(self.project_links_directory, project_name)
        try:
            FileSystemService.mkdir(project_dir)
        except FileExistsError as e:
            log.warning("Project dir: {} already exists".format(project_dir))

        for project in projects:
            try:
                link_name = os.path.join(project_dir, project.runfolder_name)
                FileSystemService.symlink(project.path, link_name)
            except FileExistsError:
                log.warning("Project link: {} already exists".format(project_dir))
                continue

        return FileSystemService.abspath(project_dir)

    def stage_runfolders_for_project(self, project_id, delivery_type):
        projects = list(self.runfolder_project_repo.get_project(project_name=project_id))

        if len(projects) < 1:
            raise ProjectNotFoundException("Could not find any Project folders"
                                           " for project name: {}".format(project_id))

        # TODO
        # If delivery type == "clean"
        #   No runfolder project can have been delivered before
        # If delivery type == "batch"
        #   Only delivery none delivered runfolders
        # If delivery type == force
        #   Re-deliver all independent of status

        # Link all <runfolder name>/Projects/<proj name>/<runfolder name> into same directory
        # TODO Continue here...
        links_directory = self._create_links_area_for_project_runfolders(project_name=project_id,
                                                                         projects=projects)

        # Stage
        staging_order = self.staging_repo.create_staging_order(source=links_directory,
                                                               status=StagingStatus.pending,
                                                               staging_target_dir=self.staging_dir)
        self.stage_order(staging_order)

        # TODO Maybe, once stage is complete remove the dir..
        return {project_id: staging_order.id}


    def create_new_stage_order(self, path):
        staging_order = self.staging_repo.create_staging_order(source=path,
                                                               status=StagingStatus.pending,
                                                               staging_target_dir=self.staging_dir)
        return staging_order

    def get_stage_order_by_id(self, stage_order_id):
        """
        Get stage order by id
        :param stage_order_id: id of StageOrder to get
        :return: the StageOrder instance
        """
        stage_order = self.staging_repo.get_staging_order_by_id(stage_order_id)
        return stage_order

    def get_status_of_stage_order(self, stage_order_id):
        """
        Get the status of a stage order
        :param stage_order_id: id of StageOrder to get
        :return: the status of the stage order, or None if not found
        """
        stage_order = self.get_stage_order_by_id(stage_order_id)
        if stage_order:
            return stage_order.status
        else:
            return None

    def kill_process_of_staging_order(self, stage_order_id):
        """
        Attempt to kill the process of the stage order.
        Will only kill stage orders which have a 'staging_in_progress' status.
        :param stage_order_id:
        :return: True if the process was killed successfully, otherwise False
        """
        session = self.session_factory()
        stage_order = self.staging_repo.get_staging_order_by_id(stage_order_id, session)

        if not stage_order:
            return False

        try:
            if stage_order.status != StagingStatus.staging_in_progress:
                raise InvalidStatusException(
                    "Can only kill processes where the staging order is 'staging_in_progress'")

            os.kill(stage_order.pid, signal.SIGTERM)

        except OSError:
            log.error("Failed to kill process with pid: {} associated with staging order: {} ".
                      format(stage_order.id, stage_order.pid))
            return False
        except InvalidStatusException:
            log.warning("Tried to kill process for staging order: {}, but didn't to it because it's status did not make"
                        "it eligible for killing.".format(stage_order.id))
            return False
        else:
            log.debug("Successfully killed process with pid: {} associated with staging order: {} ".
                      format(stage_order.id, stage_order.pid))
            stage_order.status = StagingStatus.staging_failed
            session.commit()
            return True
