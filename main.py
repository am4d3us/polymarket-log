import aiofiles, aiohttp, asyncio
import orjson, os, time

from websockets.asyncio.client import connect, ClientConnection
from websockets.exceptions import ConnectionClosedError

from pathlib import Path
from typing import Any


def info(string: str):
    print("[?]" + string)


def warn(string: str):
    print("[!]" + string)


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

        base = Daemon._get_timestamp()
        timestamps = [base + 900 * i for i in range(1, windows + 1)]

        till_next = timestamps[0] - time.time_ns() // 10**9
        info(f"sleeping till {till_next}")
        await asyncio.sleep(till_next)

        for i in range(windows):

            slug = f"{self._coin}-updown-15m-{timestamps[i]}"
            tokens = await self._get_tokens(slug)
            self._ingest.name = slug

            await self._subscribe(tokens)

            run_for = timestamps[i] + 900 - time.time_ns() // 10**9
            deadline = asyncio.get_running_loop().time() + run_for

            info("will monitor {slug} for {run_for} seconds")

            while True:
                try:
                    async with asyncio.timeout_at(deadline):
                        message = await self._socket.recv()
                        data = orjson.loads(message)
                        await self._ingest.append(data)
                except TimeoutError:
                    info("timeout for {slug}, moving onto the next window")
                    await self._ingest.flush()
                    await self._reconnect()
                    break
                except ConnectionClosedError:
                    warn(f"connection closed for {self._coin}, reconnecting")
                    await self._reconnect()
                    await self._subscribe(tokens)

            pass

    async def close(self):
        if self._socket:
            await self._socket.close()
        await self._session.close()

    async def _reconnect(self):
        await self._socket.close()
        self._socket = await connect(Daemon._PM_CLOB_ENDPOINT)

    async def _subscribe(self, tokens: tuple[str, str]):
        await self._scribe(tokens, "subscribe")

    async def _unsubscribe(self, tokens: tuple[str, str]):
        await self._scribe(tokens, "unsubscribe")

    async def _scribe(self, tokens: tuple[str, str], operation: str):
        if not self._socket:
            return

        await self._socket.send(
            orjson.dumps({"assets_ids": tokens, "operation": operation})
        )

    async def _get_tokens(self, slug: str) -> tuple[str, str]:
        async with self._session.request("GET", slug) as response:
            return await response.json()["clobTokenIds"]

    @staticmethod
    def _get_timestamp() -> int:
        timestamp = time.time_ns() // 10**9
        return (timestamp // 900) * 900


def main():
    print("Hello from polymarket-log!")


if __name__ == "__main__":
    main()
