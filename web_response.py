"""Identify real application responses, including the mounted Pro interface."""

from __future__ import annotations


class WeatherResponseMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def identified(message):
            if message["type"] == "http.response.start":
                headers = [
                    (key, value)
                    for key, value in message.get("headers", [])
                    if key.lower() != b"x-meteo-app"
                ]
                message = {**message, "headers": [*headers, (b"x-meteo-app", b"v5")]}
            await send(message)

        await self.app(scope, receive, identified)
