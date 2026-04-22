from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field


REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    repo_root: Path = REPO_ROOT
    artifacts_dir: Path = REPO_ROOT / "artifacts"
    uploads_dir: Path = REPO_ROOT / "artifacts" / "uploads"
    generated_dir: Path = REPO_ROOT / "artifacts" / "generated"
    database_path: Path = REPO_ROOT / "artifacts" / "data_views_agent.sqlite"
    office_manifest_path: Path = (
        REPO_ROOT / "src" / "data_views_agent" / "data" / "maharashtra_registration_offices.json"
    )
    gemini_api_key: str | None = Field(
        default_factory=lambda: os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    )
    gemini_model: str = Field(default_factory=lambda: os.getenv("GEMINI_MODEL", "gemini-2.5-flash"))
    default_total_rows: int = 300_000
    default_preview_rows: int = 200

    def ensure_directories(self) -> None:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.office_manifest_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv(REPO_ROOT / ".env")
    settings = Settings()
    settings.ensure_directories()
    return settings

