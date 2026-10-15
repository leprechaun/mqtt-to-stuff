from mqtt_to_stuff.storage_options import build_storage_options


def test_no_endpoint_no_token_by_default(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    assert build_storage_options() == {}


def test_endpoint_set_without_session_token(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://minio.example.org")
    assert build_storage_options(include_session_token=False) == {
        "endpoint_url": "https://minio.example.org"
    }


def test_session_token_included_when_requested_but_unset(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)

    assert build_storage_options(include_session_token=True) == {"AWS_SESSION_TOKEN": ""}


def test_endpoint_and_session_token_both_included(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://minio.example.org")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "sometoken")

    assert build_storage_options(include_session_token=True) == {
        "endpoint_url": "https://minio.example.org",
        "AWS_SESSION_TOKEN": "sometoken",
    }
