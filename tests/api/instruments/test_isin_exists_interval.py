def test_isin_exists_interval(api_client):
    response = api_client.get(
        "/api/isin_exists_interval",
        params={
            "start_date": "2021-01-01",
            "end_date": "2021-01-02",
            "instrument": "BTCETH",
            "exchange": "Binance",
        },
    )
    assert response.status_code == 200
    assert len(response.json()["payload"]) == 2
    for payload in response.json()["payload"]:
        assert payload["instrument"] == "BTCETH"
        assert payload["exchange"] == "Binance"
        assert payload["iid"] == 11
        assert payload["market_type"] == "spot"


def test_isin_exists_interval_no_data(api_client):
    response = api_client.get(
        "/api/isin_exists_interval",
        params={
            "start_date": "2021-01-01",
            "end_date": "2021-01-02",
            "instrument": "wrong",
            "exchange": "exch",
        },
    )
    assert response.status_code == 200
    assert len(response.json()["payload"]) == 0
