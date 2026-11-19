from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from memory_vault.models.user import User
from memory_vault.schemas.users import TelegramUserCreate


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, data: TelegramUserCreate) -> User:
        insert_stmt = insert(User).values(**data.model_dump())

        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[User.telegram_user_id],
            set_={
                "username": insert_stmt.excluded.username,
                "first_name": insert_stmt.excluded.first_name,
                "last_name": insert_stmt.excluded.last_name,
                "language_code": insert_stmt.excluded.language_code,
                "updated_at": func.now(),
            },
        ).returning(User)

        result = await self.session.execute(stmt)

        return result.scalar_one()
