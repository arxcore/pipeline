import aiohttp
import monitoring.exc_models as exc


class Retryable:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __call__(self) -> bool:
        # retry server errors (5xx)
        if isinstance(self.error, aiohttp.ClientResponseError):
            if self.error.status >= 500:
                return True
            if self.error.status == 429:
                return True
        # retry connection errors
        if isinstance(
            self.error, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError)
        ):
            return True
        # retry custom exception if needed
        if isinstance(self.error, exc.RateLimit):
            return True
        return False
