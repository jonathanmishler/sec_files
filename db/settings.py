from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Gets the Postgres connection settings from the local env variables or .env file"""

    db_type: str = Field("postgresql")
    username: str = Field(None, env="db_username")
    password: str = Field(None, env="db_password")
    host: str = Field("localhost", env="db_host")
    port: str = Field("5432", env="db_port")
    dbname: str = Field("", env="db_name")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        secrets_dir = "./secrets"

    @property
    def conn_str(self):
        if self.password is None:
            user_str = self.username
        else:
            user_str = f"{self.username}:{self.password}"
        return f"{self.db_type}://{user_str}@{self.host}:{self.port}/{self.dbname}"
