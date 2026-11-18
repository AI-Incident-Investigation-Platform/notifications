import asyncio
import os
from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from src.core.db.models import metadata

config = context.config
def run_offline():
    context.configure(url=os.environ['DATABASE_URL'], target_metadata=metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
async def run_online():
    engine = create_async_engine(os.environ['DATABASE_URL'])
    async with engine.connect() as connection:
        await connection.run_sync(lambda conn: context.configure(connection=conn, target_metadata=metadata))
        await connection.run_sync(lambda conn: context.run_migrations())
    await engine.dispose()
if context.is_offline_mode():
    run_offline()
else:
    asyncio.run(run_online())
