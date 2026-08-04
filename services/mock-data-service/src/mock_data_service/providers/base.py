from typing import Protocol


class MockProvider(Protocol):
    """Lifecycle implemented by independently testable mock-data domains."""

    name: str

    def bootstrap(self) -> None: ...

    def health(self) -> None: ...

    def run(self) -> None: ...
