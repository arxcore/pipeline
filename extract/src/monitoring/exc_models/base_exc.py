class PipelineCrash(Exception):
    """Root Base Exceeption"""

    pass


class ProcessingFailed(PipelineCrash):
    """Base class to catch Failed of Extrak-Tranform-Format Data"""

    pass


class RoutingError(ProcessingFailed):
    """catch Routing Process Error"""

    pass


class FetchDataError(RoutingError):
    """catch Fetching process Error"""

    pass


class RateLimit(FetchDataError):
    pass


class AuthenticationError(FetchDataError):
    pass


class BLSRequestsError(FetchDataError):
    pass


class BEARequestsError(FetchDataError):
    pass


class FREDRequestsError(FetchDataError):
    pass


class ParseDataError(RoutingError):
    """catch Parser process Error"""

    pass


class BLSParserError(ParseDataError):
    pass


class BEAParserError(ParseDataError):
    pass


class FREDParserError(ParseDataError):
    pass


class ResultsNotFound(ProcessingFailed):
    """catch Error Results Data"""

    pass


class FormatError(ProcessingFailed):
    """catch Error Format Final data"""

    pass


class UploadFailed(PipelineCrash):
    """catch Error Upload to DB"""

    pass


class ResourceNotFound(PipelineCrash):
    """catch Error Load Resource .env File"""

    pass
