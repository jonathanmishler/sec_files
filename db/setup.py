from sqlmodel import SQLModel, create_engine
from . import models # noqa
from .settings import Settings

config = Settings()

engine = create_engine(config.conn_str)

def main_setup():
    SQLModel.metadata.create_all(engine)

if __name__ == "__main__":
    main_setup()