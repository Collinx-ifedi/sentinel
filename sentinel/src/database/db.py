# database/db.py
import os
import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncEngine
)

# Setup basic logging for DB events
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 1. DATABASE URL CONFIGURATION
# -----------------------------------------------------------------------------
# Pull from .env for production (e.g., PostgreSQL: postgresql+asyncpg://user:pass@host/db)
# Fallback to async SQLite for local development and testing.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./sentinel_agent.db")

# -----------------------------------------------------------------------------
# 2. ASYNC ENGINE CREATION
# -----------------------------------------------------------------------------
# echo=False prevents dumping SQL queries to the terminal in production
# Connect args configured to allow multi-threading in SQLite if used.
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True, # Enforce SQLAlchemy 2.0 semantics
    connect_args=connect_args
)

# -----------------------------------------------------------------------------
# 3. SESSION MAKER FACTORY
# -----------------------------------------------------------------------------
# expire_on_commit=False is CRITICAL for async SQLAlchemy. 
# It prevents the session from expiring objects after a commit, 
# which would cause an ImplicitIOError if the AI tries to read them again.
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

# -----------------------------------------------------------------------------
# 4. FASTAPI DEPENDENCY INJECTION
# -----------------------------------------------------------------------------
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency to be injected into FastAPI/Starlette routes.
    Ensures a new session is created per request and safely closed afterward.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# -----------------------------------------------------------------------------
# 5. INITIALIZATION HELPER
# -----------------------------------------------------------------------------
async def init_db() -> None:
    """
    Creates all tables based on the models defined.
    Note: In a true enterprise production environment, you would use 'Alembic' 
    for database migrations instead of calling create_all().
    """
    from .models import Base  # Import here to avoid circular dependencies
    
    try:
        async with engine.begin() as conn:
            # Create tables if they don't exist
            await conn.run_sync(Base.metadata.create_all)
            log.info("Database tables verified/created successfully.")
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        raise
