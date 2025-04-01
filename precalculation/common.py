def parse_yes_no_entry(s: str | bool) -> bool:
    """Parse a yes/no entry."""
    if type(s) == bool:
        return s

    if s is None:
        return False  # default to False if no value is provided
    v = s.lower()
    if v == 'y' or v == 'yes' or v == 'p':  # p == probably, we take it as true
        return True
    return False