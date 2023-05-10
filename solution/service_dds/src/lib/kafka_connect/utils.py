from json import JSONEncoder
from uuid import UUID


class UUIDEncoder(JSONEncoder):
    """JSONEncoder for UUID type."""
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return JSONEncoder.default(self, obj)