
import subprocess
import logging

from delivery.models.execution import ExecutionResult, Execution

log = logging.getLogger(__name__)


class ExternalProgramService(object):
    """
    A service for running external programs
    """

    @staticmethod
    def run(cmd):
        """
        Run a process and do not wait for it to finish
        :param cmd: the command to run as a list, i.e. ['ls','-l', '/']
        :return: A instance of Execution
        """
        log.debug("Running command: {}".format(" ".join(cmd)))
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE)
        return Execution(pid=p.pid, process_obj=p)

    @staticmethod
    def wait_for_execution(execution):
        """
        Wait for an execution to finish
        :param execution: instance of Execution
        :return: an ExecutionResult for the execution
        """
        out, err = execution.process_obj.communicate()
        status_code = execution.process_obj.wait()
        return ExecutionResult(out, err, status_code)

    @staticmethod
    def run_and_wait(cmd):
        """
        Run an external command and wait for it to finish
        :param cmd: the command to run as a list, i.e. ['ls','-l', '/']
        :return: an ExecutionResult for the execution
        """
        execution = ExternalProgramService.run(cmd)
        return ExternalProgramService.wait_for_execution(execution)
