import pytest
from pydantic import SecretStr, ValidationError

from memory_vault.config import Settings


def _make_settings(**overrides: float) -> Settings:
    if "outbox_shutdown_timeout_seconds" in overrides:
        return Settings(
            postgres_password=SecretStr("secret"),
            telegram_bot_token=SecretStr("token"),
            outbox_shutdown_timeout_seconds=overrides["outbox_shutdown_timeout_seconds"],
        )

    return Settings(
        postgres_password=SecretStr("secret"),
        telegram_bot_token=SecretStr("token"),
    )


def test_outbox_shutdown_timeout_defaults_to_positive_value() -> None:
    settings = _make_settings()

    assert settings.outbox_shutdown_timeout_seconds == 30.0


def test_outbox_shutdown_timeout_accepts_positive_value() -> None:
    settings = _make_settings(outbox_shutdown_timeout_seconds=5.5)

    assert settings.outbox_shutdown_timeout_seconds == 5.5


@pytest.mark.parametrize("value", [0, -1.0])
def test_outbox_shutdown_timeout_rejects_non_positive_values(value: float) -> None:
    with pytest.raises(ValidationError):
        _make_settings(outbox_shutdown_timeout_seconds=value)
