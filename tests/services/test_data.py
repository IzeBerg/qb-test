import datetime
import os
import random
import tempfile

import pytest

from services.data import generate_dates, get_date_path, read_chunks

# only utility functions are covered, other will be covered in api tests


@pytest.mark.asyncio
async def test_read_chunks(random_file):
    chunks = [len(chunk) async for chunk in read_chunks(random_file, 50)]
    assert chunks == [50, 50, 20]


def test_generate_dates():
    dates = generate_dates(datetime.date(2021, 1, 1), datetime.date(2021, 1, 3))
    assert list(dates) == [
        datetime.date(2021, 1, 1),
        datetime.date(2021, 1, 2),
        datetime.date(2021, 1, 3),
    ]

    dates = generate_dates(datetime.date(2021, 1, 1), datetime.date(2021, 1, 1))
    assert list(dates) == [
        datetime.date(2021, 1, 1),
    ]


def test_get_date_path():
    path = get_date_path(datetime.date(2021, 12, 31))
    assert path == "data/2021/12/31"


@pytest.fixture()
def random_file():
    filename = tempfile.mktemp()
    with open(filename, "wb") as f:
        f.write(random.randbytes(120))

    yield filename

    os.remove(filename)
