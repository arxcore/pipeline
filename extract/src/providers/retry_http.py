import aiohttp


class Retryable:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __call__(self) -> bool:
        if isinstance(self.error, aiohttp.ClientResponseError):
            return self.error.status >= 500
        return isinstance(
            self.error, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError)
        )
