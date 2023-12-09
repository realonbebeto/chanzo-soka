from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_username: str
    db_password: str
    db_host: str
    db_port: str
    db_name: str

    class Config:
        env_file = "/Users/realonbebeto/Kazispace/local/py/scrftbl/.env"


config = Settings()
