class Settings(BaseSettings):

    s3_endpoint: AnyHttpUrl = "http://127.0.0.1:9000"
    s3_access_key: str = "admin"
    s3_secret_key: str = "admin"
    s3_bucket: str = "name"
    s3_region: str = "us-east-1"

    s3_client_connect_timeout_s: int = 5
    s3_client_read_timeout_s: int = 20
    s3_http_pool_max_size: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
