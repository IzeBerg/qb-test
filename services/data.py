import datetime
import os
from typing import AsyncIterator, Iterator
from xml.etree import ElementTree

import anyio
from pydantic import BaseModel

DATA_PATH = "data"


class Payload(BaseModel):
    instrument: str
    exchange: str
    iid: int
    market_type: str


async def get_isin(
    start_date: datetime.date,
    end_date: datetime.date,
    instrument: str | None,
    exchange: str | None,
) -> list[Payload]:
    payloads = []

    for date in generate_dates(start_date, end_date):
        try:
            meta = await get_manifest(date)
        except FileNotFoundError:
            continue

        for exch in meta.findall("./*/Exchange"):
            exchange_name, market_type = exch.attrib["Name"].split(".")
            if exchange and exchange_name != exchange:
                continue

            for inst in exch.findall("./*/Instrument"):
                if instrument and inst.attrib["Name"] != instrument:
                    continue

                payloads.append(
                    Payload(
                        instrument=inst.attrib["Name"],
                        exchange=exchange_name,
                        iid=int(inst.attrib["Iid"]),
                        market_type=market_type,
                    )
                )

    return payloads


async def find_data_filename(
    date: datetime.date,
    iid: int | None,
    instrument: str | None,
    exchange: str | None,
) -> str:
    manifest = await get_manifest(date)
    for exch in manifest.findall("./*/Exchange"):
        exch_full_name = exch.attrib["Name"]
        exchange_name, market_type = exch_full_name.split(".")
        if exchange and exchange_name != exchange:
            continue

        for inst in exch.findall("./*/Instrument"):
            inst_name = inst.attrib["Name"]
            if instrument and inst_name != instrument:
                continue

            if iid and int(inst.attrib["Iid"]) != iid:
                continue

            return f"{inst_name}@{exch_full_name}.dat"

    raise FileNotFoundError


async def get_manifest(date: datetime.date) -> ElementTree.Element:
    async with await anyio.open_file(
        os.path.join(get_date_path(date), "manifest.xml"),
        mode="r",
    ) as fp:
        return ElementTree.fromstring(await fp.read())


def get_date_path(date: datetime.date) -> str:
    return os.path.join(DATA_PATH, date.strftime("%Y/%-m/%-d"))


def generate_dates(begin: datetime.date, end: datetime.date) -> Iterator[datetime.date]:
    current = None
    while current is None or current <= end:
        if current is None:
            current = begin
        yield current
        current += datetime.timedelta(days=1)


async def read_chunks(filepath: str, chunk_size: int) -> AsyncIterator[bytes]:
    async with await anyio.open_file(filepath, mode="rb") as fp:
        while chunk := await fp.read(chunk_size):
            yield chunk
