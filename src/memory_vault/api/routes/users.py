from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from memory_vault.database import get_session
from memory_vault.schemas.users import TelegramUserCreate, UserResponse
from memory_vault.services.users import UserService

router = APIRouter(prefix="/users", tags=["users"])


@router.post("/register", response_model=UserResponse)
async def register_user(
    data: TelegramUserCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> UserResponse:
    user = await UserService(session).register(data)

    return UserResponse(
        id=str(user.id),
        telegram_user_id=user.telegram_user_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language_code=user.language_code,
    )
