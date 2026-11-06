import asyncio
from typing import Any

import requests


async def request_json(method: str, url: str, **kwargs: Any) -> Any:
    """Keep blocking requests and response parsing off the event loop."""
    kwargs.setdefault("timeout", 10)

    def send():
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    return await asyncio.to_thread(send)
