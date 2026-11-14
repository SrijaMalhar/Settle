"""Store failures. Either one means this process may no longer own its shard: exit, never reconnect."""


class StoreFailed(Exception):
    """The database could not be reached or refused the write."""


class Conflict(StoreFailed):
    """Someone else already wrote this payment's next event: there is a second writer."""
