from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AppConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    imap_host: str = ""
    imap_port: int = Field(default=993, ge=1, le=65535)
    imap_ssl: bool = True
    imap_user: str = ""
    imap_pass: str = ""
    imap_folder: str = "INBOX"
    headless: bool = True
    captcha_timeout_sec: int = Field(default=120, ge=1, le=3600)
    email_poll_timeout_sec: int = Field(default=90, ge=1, le=3600)
    default_password: str = ""
    auto_verify_email: bool = True
    concurrency: int = Field(default=2, ge=1, le=10)
    proxy_enabled: bool = False
    proxy_strategy: Literal["round_robin", "random"] = "round_robin"
    proxy_pool: str = ""
    proxy_server: str = ""
    proxy_username: str = ""
    proxy_password: str = ""


class BatchStartRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    concurrency: int | None = Field(default=None, ge=1, le=10)
