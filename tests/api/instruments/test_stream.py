def test_stream_invalid_chunk_size(api_client):
    response = api_client.get(
        "/api/stream",
        params={
            "date": "2021-01-01",
            "chunk_size": 0,
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "greater_than_equal"
    assert response.json()["detail"][0]["loc"] == ["query", "chunk_size"]


def test_stream_invalid_no_params(api_client):
    response = api_client.get(
        "/api/stream",
        params={
            "date": "2021-01-01",
            "chunk_size": 10,
        },
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "iid or instrument/exchange must be provided"

    response = api_client.get(
        "/api/stream",
        params={
            "date": "2021-01-01",
            "chunk_size": 10,
            "exchange": "Binance",
        },
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "iid or instrument/exchange must be provided"

    response = api_client.get(
        "/api/stream",
        params={
            "date": "2021-01-01",
            "chunk_size": 10,
            "instrument": "BTCETH",
        },
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "iid or instrument/exchange must be provided"


def test_stream_invalid_all_params(api_client):
    response = api_client.get(
        "/api/stream",
        params={
            "date": "2021-01-01",
            "chunk_size": 10,
            "iid": 11,
            "instrument": "BTCETH",
            "exchange": "Binance",
        },
    )
    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "iid and instrument/exchange cannot be used together"
    )


def test_stream_invalid_no_data(api_client):
    response = api_client.get(
        "/api/stream",
        params={
            "date": "2020-01-01",
            "chunk_size": 10,
            "iid": 123,
        },
    )
    assert response.status_code == 404


def test_stream_valid(api_client):
    response = api_client.get(
        "/api/stream",
        params={
            "date": "2021-01-01",
            "chunk_size": 1024,
            "iid": 11,
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/octet-stream"
    assert (
        response.headers["content-disposition"]
        == "attachment; filename=BTCETH@Binance.spot.dat"
    )
    assert "content-length" not in response.headers
    assert len(response.content) == 1024
