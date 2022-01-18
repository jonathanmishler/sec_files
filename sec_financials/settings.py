from pathlib import Path
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    """Gets the Postgres connection settings from the local env variables or .env file"""

    fin_download_dir: str = Field("./files/")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def dir_path(self) -> Path:
        return Path(self.fin_download_dir)