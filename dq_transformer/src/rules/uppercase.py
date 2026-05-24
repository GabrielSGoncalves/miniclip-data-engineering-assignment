from rules.base import Rule


class UppercaseRule(Rule):
    def apply(self, event: dict) -> dict:
        if self.field in event:
            return {**event, self.field: str(event[self.field]).upper()}
        return event
