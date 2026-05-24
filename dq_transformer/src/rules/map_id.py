from rules.base import Rule


class MapIdRule(Rule):
    def __init__(self, field: str, event_types: list[str], mapping: dict):
        super().__init__(field, event_types)
        self.mapping = {str(k): v for k, v in mapping.items()}

    def apply(self, event: dict) -> dict:
        if self.field in event:
            original = str(event[self.field])
            return {**event, self.field: self.mapping.get(original, event[self.field])}
        return event
