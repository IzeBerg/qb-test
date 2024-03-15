def test_isin_exists(api_client):
    response = api_client.get(
        "/api/isin_exists",
        params={
            "date": "2021-01-01",
            "instrument": "BTCETH",
            "exchange": "Binance",
        },
    )
    assert response.status_code == 200
    assert len(response.json()["payload"]) == 1
    for payload in response.json()["payload"]:
        assert payload["instrument"] == "BTCETH"
        assert payload["exchange"] == "Binance"
        assert payload["iid"] == 11
        assert payload["market_type"] == "spot"


def test_isin_exists_no_data(api_client):
    response = api_client.get(
        "/api/isin_exists",
        params={
            "date": "2020-01-01",
        },
    )
    assert response.status_code == 200
    assert len(response.json()["payload"]) == 0
