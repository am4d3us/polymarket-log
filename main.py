import aiofiles, aiohttp, asyncio
import orjson, os

from pathlib import Path
from typing import Any
from websockets.asyncio.client import connect, ClientConnection


class Ingest:
    def __init__(self, name: str, directory: str | None, until_flush: int = 1000):
        self._set: list[dict[str, Any]] = []
        self._number_of_logs = 0

        self._name = name
        self._directory = directory
        self._until_flush = until_flush

        filename = Path(f"{name}.jsonl")
        self._filepath = Path(directory) / filename if directory else filename

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

        filename = Path(f"{name}.jsonl")
        self.__filepath = (
            Path(self._directory) / filename if self._directory else filename
        )

        self.clear()

    def clear(self) -> None:
        self._set.clear()
        self._number_of_logs = 0

    async def append(self, data: dict[str, Any]) -> None:
        self._set.append(data)
        self._number_of_logs += 1

        if self._number_of_logs < self._until_flush:
            return

        await self.flush()

    async def flush(self) -> None:
        async with aiofiles.open(self.__filepath, mode="ab") as file:
            for data in map(orjson.dumps, self._set):
                await file.write(data)
                await file.write(os.linesep.encode())

        self.clear()


class Daemon:
    _PM_GAMMA_ENDPOINT = "https://gamma-api.polymarket.com/markets/slug/"
    _PM_CLOB_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, coin: str, directory: str | None):
        self._coin = coin
        self._socket: ClientConnection | None = None
        self._session = aiohttp.ClientSession(Daemon._PM_GAMMA_ENDPOINT)
        self._ingest = Ingest(coin, directory)

    async def capture(self, windows: int = 1):
        self._socket = await connect(Daemon._PM_CLOB_ENDPOINT)

    async def close(self):
        if self._socket:
            await self._socket.close()
        await self._session.close()


def main():
    print("Hello from polymarket-log!")


if __name__ == "__main__":
    main()
