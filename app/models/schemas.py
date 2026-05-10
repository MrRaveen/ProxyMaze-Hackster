from __future__ import annotations
import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, Integer, String, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def generate_webhook_id() -> str:
    return f"wh-{uuid.uuid4().hex[:8]}"


class Base(DeclarativeBase):
    pass


class SerializableMixin:
    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {}
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            if isinstance(value, datetime):
                # Enforce strict ISO 8601 UTC with 'Z' suffix as per Chapter 7
                data[key] = value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                data[key] = value
        return data


class Config(SerializableMixin, Base):
    __tablename__ = 'config'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    check_interval_seconds: Mapped[int] = mapped_column(Integer, default=15, nullable=False)
    request_timeout_ms: Mapped[int] = mapped_column(Integer, default=3000, nullable=False)


class Proxy(SerializableMixin, Base):
    __tablename__ = 'proxy'

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[str] = mapped_column(String(10), default='pending')
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    
    # Tracked for the GET /proxies/{id} Dossier payload
    total_checks: Mapped[int] = mapped_column(Integer, default=0)
    successful_checks: Mapped[int] = mapped_column(Integer, default=0)


class CheckResult(SerializableMixin, Base):
    __tablename__ = 'check_result'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # No ForeignKey constraint here. Chapter 08 requires history to survive pool deletion.
    proxy_id: Mapped[str] = mapped_column(String(255), nullable=False) 
    status: Mapped[str] = mapped_column(String(5), nullable=False) # 'up' or 'down'
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class Alert(SerializableMixin, Base):
    __tablename__ = 'alert'

    alert_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(10), default='active', nullable=False)
    failure_rate: Mapped[float] = mapped_column(Float, nullable=False)
    total_proxies: Mapped[int] = mapped_column(Integer, nullable=False)
    failed_proxies: Mapped[int] = mapped_column(Integer, nullable=False)
    failed_proxy_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    threshold: Mapped[float] = mapped_column(Float, default=0.20, nullable=False)
    fired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str] = mapped_column(String(255), nullable=False)


class Webhook(SerializableMixin, Base):
    __tablename__ = 'webhook'

    webhook_id: Mapped[str] = mapped_column(String(50), primary_key=True, default=generate_webhook_id)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    integration_type: Mapped[str] = mapped_column(String(20), default='generic', nullable=False) # slack/discord/generic
    username: Mapped[str | None] = mapped_column(String(100), nullable=True)
    events: Mapped[list[str]] = mapped_column(JSON, default=lambda: ["alert.fired", "alert.resolved"], nullable=False)


class WebhookDelivery(Base):
    __tablename__ = 'webhook_delivery'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    webhook_id: Mapped[str] = mapped_column(String(50), nullable=False)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)