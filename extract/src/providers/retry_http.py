import aiohttp
import monitoring.exc_models as exc


class Retryable:
    def __call__(self, error: BaseException) -> bool:
        # retry server errors (5xx)
        if isinstance(error, aiohttp.ClientResponseError):
            if error.status >= 500:
                return True
            if error.status == 429:
                return True
        # retry connection errors
        if isinstance(
            error, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError)
        ):
            return True
        # retry custom exception if needed
        if isinstance(error, exc.RateLimit):
            return True
        return False
