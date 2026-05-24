from abc import ABC, abstractmethod


class Rule(ABC):
    def __init__(self, field: str, event_types: list[str]):
        self.field = field
        self.event_types = event_types or []

    def applies_to(self, event: dict) -> bool:
        if not self.event_types:
            return True
        return event.get("event-type") in self.event_types

    @abstractmethod
    def apply(self, event: dict) -> dict: ...
