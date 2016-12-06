

class RunfolderNotFoundException(Exception):
    """
    Should be raised when a runfolder is not found
    """
    pass


class ProjectNotFoundException(Exception):
    """
    Should be raised when and invalid or non-existent project is searched for.
    """
    pass

class ToManyProjectsFound(Exception):
    """
    Should be raise when to many projects match some specific criteria
    """

class InvalidStatusException(Exception):
    """
    Should be raised when an object is found to be in a invalid state, e.g. if the program tries to start staging
    on a StagingOrder which is already `in_progress`
    """
    pass
