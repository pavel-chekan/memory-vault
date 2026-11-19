from sqlalchemy.ext.asyncio import AsyncSession

from memory_vault.models.user import User
from memory_vault.repositories.users import UserRepository
from memory_vault.schemas.users import TelegramUserCreate


class UserService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.repository = UserRepository(session)

    async def register(
        self,
        data: TelegramUserCreate,
    ) -> User:
        async with self.session.begin():
            user = await self.repository.upsert(data)

        return user
