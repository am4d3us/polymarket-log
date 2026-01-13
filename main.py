import aiofiles, aiohttp, asyncio
import argparse, orjson, os, time

from websockets.asyncio.client import connect, ClientConnection
from websockets.exceptions import ConnectionClosedError

from pathlib import Path
from typing import Any


def info(string: str):
    print("[?] " + string)


def warn(string: str):
    print("[!] " + string)


class Ingest:
    def __init__(self, name: str, directory: Path | None, until_flush: int = 1000):
        self._set: list[dict[str, Any]] = []
        self._number_of_logs = 0

        self._name = name
        self._until_flush = until_flush

        filename = Path(f"{name}.jsonl")
        if directory:
            self._directory = directory
            self._directory.mkdir(exist_ok=True)
            self._filepath = self._directory / filename
        else:
            self._filepath = filename

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

        filename = Path(f"{name}.jsonl")
        self._filepath = self._directory / filename if self._directory else filename

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
        async with aiofiles.open(self._filepath, mode="ab") as file:
            for data in map(orjson.dumps, self._set):
                await file.write(data)
                await file.write(os.linesep.encode())

        self.clear()


class Daemon:
    _PM_GAMMA_ENDPOINT = "https://gamma-api.polymarket.com/markets/slug/"
    _PM_CLOB_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, coin: str, directory: Path | None = None):
        self._coin = coin
        self._socket: ClientConnection | None = None
        self._session = aiohttp.ClientSession(Daemon._PM_GAMMA_ENDPOINT)
        self._ingest = Ingest(coin, directory)

    @property
    def coin(self) -> str:
        return self._coin

    async def capture(self, windows: int = 1) -> None:
        base = Daemon._get_timestamp()
        timestamps = [base + 900 * i for i in range(1, windows + 1)]

        till_next = timestamps[0] - time.time_ns() // 10**9
        info(f"sleeping for {till_next} seconds till next {self.coin} window")
        await asyncio.sleep(till_next)

        self._socket = await connect(Daemon._PM_CLOB_ENDPOINT)

        for i in range(windows):
            slug = f"{self._coin}-updown-15m-{timestamps[i]}"
            tokens = await self._get_tokens(slug)
            self._ingest.name = slug

            await self._subscribe(tokens)

            run_for = timestamps[i] + 900 - time.time_ns() // 10**9
            deadline = asyncio.get_running_loop().time() + run_for

            info(f"will monitor {slug} for {run_for} seconds")

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
                except ConnectionClosedError as e:
                    warn(f"connection closed for {self._coin}, reconnecting: {e}")
                    await self._reconnect()
                    await self._subscribe(tokens)

            pass

    async def close(self) -> None:
        if self._socket:
            await self._socket.close()
        await self._session.close()

    async def _reconnect(self) -> None:
        if self._socket:
            await self._socket.close()
        self._socket = await connect(Daemon._PM_CLOB_ENDPOINT)

    async def _subscribe(self, tokens: tuple[str, str]) -> None:
        await self._scribe(tokens, "subscribe")

    async def _unsubscribe(self, tokens: tuple[str, str]) -> None:
        await self._scribe(tokens, "unsubscribe")

    async def _scribe(self, tokens: tuple[str, str], operation: str) -> None:
        if not self._socket:
            warn(f"unable to {operation} sinec there is no socket")
            return

        await self._socket.send(
            orjson.dumps({"assets_ids": tokens, "operation": operation})
        )

    async def _get_tokens(self, slug: str) -> tuple[str, str]:
        async with self._session.request("GET", slug) as response:
            return orjson.loads((await response.json())["clobTokenIds"])

    @staticmethod
    def _get_timestamp() -> int:
        timestamp = time.time_ns() // 10**9
        return (timestamp // 900) * 900


async def main():
    parser = argparse.ArgumentParser(
        description="Log and save real-time transactions in 15-minute Polymarket windows for the given coin(s)."
    )
    parser.add_argument(
        "coins",
        help="The coin(s) one wishes to monitor, and thus log.",
        nargs="+",
        default=[],
    )
    parser.add_argument(
        "-w",
        "--windows",
        help="The number of windows to log.",
        nargs="?",
        type=int,
        default=1,
    )
    parser.add_argument(
        "-d",
        "--directory",
        help="The directory where the logs will be stored.",
        nargs="?",
        type=Path,
    )

    arguments = parser.parse_args()

    windows = arguments.windows
    directory = arguments.directory
    daemons = [Daemon(coin, directory) for coin in arguments.coins]

    info(f"set to monitor {[daemon.coin for daemon in daemons]}")

    async with asyncio.TaskGroup() as tg:
        _tasks = [tg.create_task(daemon.capture(windows)) for daemon in daemons]

    async with asyncio.TaskGroup() as tg:
        _tasks = [tg.create_task(daemon.close()) for daemon in daemons]


if __name__ == "__main__":
    asyncio.run(main())
