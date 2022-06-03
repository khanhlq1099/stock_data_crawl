class SsiApiException(Exception):
    error_code: str
    message: str

    def __init__(self, error_code: str, message: str, *args: object) -> None:
        self.error_code = error_code
        self.message = message
        super().__init__(*args)

    def __str__(self) -> str:
        return self.message
