from pydantic import BaseModel, ConfigDict, Field


class TelegramUserCreate(BaseModel):
    telegram_user_id: int
    username: str | None = Field(default=None, max_length=64)
    first_name: str = Field(min_length=1, max_length=128)
    last_name: str | None = Field(default=None, max_length=128)
    language_code: str | None = Field(default=None, max_length=16)


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    telegram_user_id: int
    username: str | None
    first_name: str
    last_name: str | None
    language_code: str | None
