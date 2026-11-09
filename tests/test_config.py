import pytest

from app.config import Settings


@pytest.fixture
def secrets_dir(tmp_path, monkeypatch):
    for name in ("API_KEY", "WEBHOOK_SECRET", "WEBHOOK_ALLOWED_HOSTS"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / "api_key").write_text("key-from-secret\n")
    (tmp_path / "webhook_secret").write_text("hmac-from-secret\n")
    return tmp_path


def test_secrets_are_read_from_files(secrets_dir):
    settings = Settings(_secrets_dir=secrets_dir, _env_file=None)

    assert settings.api_key.get_secret_value() == "key-from-secret"
    assert settings.webhook_secret.get_secret_value() == "hmac-from-secret"
    assert "key-from-secret" not in repr(settings)


def test_environment_overrides_secret_file(secrets_dir, monkeypatch):
    monkeypatch.setenv("API_KEY", "key-from-env")
    assert Settings(_secrets_dir=secrets_dir, _env_file=None).api_key.get_secret_value() == "key-from-env"


def test_allowed_hosts_are_comma_separated(secrets_dir, monkeypatch):
    monkeypatch.setenv("WEBHOOK_ALLOWED_HOSTS", " Webhook-Mock , other ,")
    settings = Settings(_secrets_dir=secrets_dir, _env_file=None)
    assert settings.webhook_allowed_hosts == {"webhook-mock", "other"}
