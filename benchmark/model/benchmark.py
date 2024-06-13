from dataclasses import dataclass


@dataclass(frozen=True)
class Benchmark:
    """
    A Benchmark that can be tested.
    """
    Query: str
    Source: str
    Type: str
    CorrectType: str
    CorrectID: str
    CorrectLabel: str
    Notes: dict[str, set[str]]

    # A string representation of this test row.
    def __str__(self):
        return f"Benchmark of query '{self.Query}' with type {self.Type} from source {self.Source}, " + \
            f"which should have the correct ID {self.CorrectID} ('{self.CorrectLabel}', type {self.CorrectType}): " + \
            str(self.Notes)
