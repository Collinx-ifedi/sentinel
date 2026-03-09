# ==============================================================================
# MODULE: DATABASE MODELS (THE LEDGER)
# VERSION: 4.0.0 "GOD MODE"
# DESCRIPTION: SQLAlchemy Object Relational Mapping (ORM) for the Sentinel Protocol.
#              Defines strict relational integrity for Users, Wallets, and Trades.
# ==============================================================================

from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey, Integer, Float
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime, timezone
import uuid

# Initialize Declarative Base
Base = declarative_base()

def generate_uuid() -> str:
    """Generates a secure, random UUIDv4 string for primary keys."""
    return str(uuid.uuid4())

def utc_now() -> datetime:
    """Returns the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)

# ==============================================================================
# 1. USER IDENTITY MODEL
# ==============================================================================
class User(Base):
    """
    Core User Model. 
    Secured via Argon2id hashing and Brevo 2FA OTP.
    """
    __tablename__ = "users"
    
    id = Column(String, primary_key=True, default=generate_uuid, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    
    # 🛡️ ARGON2 SECURITY UPDATE
    # Stores the un-truncated Argon2id hash string ($argon2id$v=19$m=65536...)
    hashed_password = Column(String, nullable=False) 
    
    # ✉️ BREVO 2FA / OTP FIELDS
    otp_code = Column(String, nullable=True)
    otp_expiry = Column(DateTime(timezone=True), nullable=True)
    is_verified = Column(Boolean, default=False)
    
    # Audit trail
    created_at = Column(DateTime(timezone=True), default=utc_now)
    
    # --- Relationships ---
    # cascade="all, delete-orphan" ensures wiping a user also wipes their keys and trade history
    wallet = relationship("AgentWallet", back_populates="user", uselist=False, cascade="all, delete-orphan")
    trades = relationship("TradeHistory", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User(email='{self.email}', is_verified={self.is_verified})>"


# ==============================================================================
# 2. ENCRYPTED WALLET MODEL
# ==============================================================================
class AgentWallet(Base):
    """
    The secure storage layer for the Sentinel Executor's hands.
    Private keys are mathematically protected via AES-256 PBKDF2.
    """
    __tablename__ = "agent_wallets"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False)
    
    # 🔐 SOLANA ON-CHAIN DATA
    public_key = Column(String, nullable=False, unique=True, index=True)
    
    # 🔐 AES-256 ENCRYPTED PAYLOAD
    encrypted_privkey = Column(String, nullable=False) 
    
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
    
    # --- Relationships ---
    user = relationship("User", back_populates="wallet")

    def __repr__(self):
        return f"<AgentWallet(public_key='{self.public_key[:8]}...')>"


# ==============================================================================
# 3. TRADE HISTORY LEDGER
# ==============================================================================
class TradeHistory(Base):
    """
    Immutable ledger of all AI-orchestrated trades executed on the blockchain.
    """
    __tablename__ = "trade_history"
    
    id = Column(String, primary_key=True, default=generate_uuid, index=True)
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # 🔗 BLOCKCHAIN PROOF
    signature = Column(String, unique=True, index=True, nullable=False) # The Solana tx signature
    
    # 📊 TRADE METRICS
    input_mint = Column(String, nullable=False)
    output_mint = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    side = Column(String, nullable=False) # "BUY" or "SELL"
    
    # execution status ("SUCCESS", "FAILED")
    status = Column(String, default="SUCCESS", nullable=False) 
    
    timestamp = Column(DateTime(timezone=True), default=utc_now)
    
    # --- Relationships ---
    user = relationship("User", back_populates="trades")

    def __repr__(self):
        return f"<TradeHistory(side='{self.side}', amount={self.amount}, status='{self.status}')>"