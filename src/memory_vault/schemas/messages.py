from pydantic import AwareDatetime, BaseModel, Field

from memory_vault.schemas.users import TelegramUserCreate

# Telegram caps text messages at 4096 UTF-16 code units. Counting code points
# gives a safe upper bound: every code point is at least one UTF-16 unit, so a
# valid Telegram message can never exceed this limit.
MAX_MESSAGE_TEXT_LENGTH = 4096


class TelegramMessageCreate(BaseModel):
    telegram_update_id: int = Field(gt=0)
    # No positivity check: group and channel chat ids are negative.
    telegram_chat_id: int
    telegram_message_id: int = Field(gt=0)
    text: str = Field(min_length=1, max_length=MAX_MESSAGE_TEXT_LENGTH)
    message_date: AwareDatetime
    user: TelegramUserCreate
