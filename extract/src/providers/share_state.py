import asyncio


shared_state: dict[str, asyncio.Event] = {}


class ExternalLimit:
    @staticmethod
    def get(provider: str):
        if provider not in shared_state:
            shared_state[provider] = asyncio.Event()
        return shared_state[provider]

    @staticmethod
    def clear():
        for event in shared_state.values():
            event.clear()
