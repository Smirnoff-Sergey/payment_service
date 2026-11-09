class PermanentProcessingError(Exception):
    """Retrying will not help: the message goes straight to the DLQ."""
