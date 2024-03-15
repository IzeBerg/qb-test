import datetime
import os

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from services import data
from services.data import Payload

router = APIRouter()


class PayloadResponse(BaseModel):
    payload: list[Payload]


@router.get("/isin_exists")
async def api_isin_exists(
    date: datetime.date,
    instrument: str | None = None,
    exchange: str | None = None,
) -> PayloadResponse:
    return PayloadResponse(
        payload=await data.get_isin(
            start_date=date,
            end_date=date,
            instrument=instrument,
            exchange=exchange,
        ),
    )


@router.get("/isin_exists_interval")
async def api_isin_exists_interval(
    start_date: datetime.date,
    end_date: datetime.date,
    instrument: str | None = None,
    exchange: str | None = None,
) -> PayloadResponse:
    return PayloadResponse(
        payload=await data.get_isin(
            start_date=start_date,
            end_date=end_date,
            instrument=instrument,
            exchange=exchange,
        ),
    )


@router.get("/stream")
async def api_stream_data(
    date: datetime.date,
    iid: int | None = None,
    instrument: str | None = None,
    exchange: str | None = None,
    chunk_size: int = Query(1024, ge=1),
) -> StreamingResponse:
    if not iid and not (instrument and exchange):
        raise HTTPException(
            status_code=400,
            detail="iid or instrument/exchange must be provided",
        )
    if iid and (instrument or exchange):
        raise HTTPException(
            status_code=400,
            detail="iid and instrument/exchange cannot be used together",
        )

    try:
        filename = await data.find_data_filename(
            date=date,
            iid=iid,
            instrument=instrument,
            exchange=exchange,
        )
        return StreamingResponse(
            content=data.read_chunks(
                chunk_size=chunk_size,
                filepath=os.path.join(data.get_date_path(date), filename),
            ),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
