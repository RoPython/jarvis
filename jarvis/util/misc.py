"""misc: miscellaneous utilities"""


import uuid


def get_uid(size=4):
    """Return a custom unique identifier."""
    uid = uuid.uuid4()
    return str(uid)[:size]
