import datetime
import os
import random

import pytest
from fastapi.testclient import TestClient

from app import app
from services.data import get_date_path


@pytest.fixture
def api_client():
    with TestClient(app) as client:
        yield client


@pytest.fixture(autouse=True)
def fakedata():
    for date in [datetime.date(2021, 1, 1), datetime.date(2021, 1, 2)]:
        raw = f"""
        <ManifestRoot>
          <Date>{date.isoformat()}</Date>
          <Exchanges>
            <Exchange Name="Binance.spot" Location="helsinki">
              <Instruments>
                <Instrument Name="BTCETH" StorageType="compressed" Levels="[0, 1, 2, 3]" Iid="11" AvailableIntervalBegin="15:49" AvailableIntervalEnd="19:31"/>
              </Instruments>
            </Exchange>
            <Exchange Name="Okex.spot" Location="moscow">
              <Instruments>
                <Instrument Name="BTCETH" StorageType="compressed" Levels="[1, 2]" Iid="146" AvailableIntervalBegin="10:12" AvailableIntervalEnd="23:23"/>
              </Instruments>
            </Exchange>
            <Exchange Name="Binance.fut" Location="sidney">
              <Instruments>
                <Instrument Name="BTCETH_PERP" StorageType="lite" Levels="[0, 1, 2, 3]" Iid="174" AvailableIntervalBegin="7:7" AvailableIntervalEnd="19:13"/>
              </Instruments>
            </Exchange>
          </Exchanges>
        </ManifestRoot>
        """

        date_path = get_date_path(date)
        os.makedirs(date_path, exist_ok=True)

        with open(os.path.join(date_path, "manifest.xml"), "w") as f:
            f.write(raw)

        for datname in [
            "BTCETH@Binance.spot.dat",
            "BTCETH_PERP@Binance.spot.dat",
            "BTCETH@Okex.spot.dat",
        ]:
            with open(os.path.join(date_path, datname), "wb") as f:
                f.write(random.randbytes(1024))
