class BaseError(Exception):
    """Basisklasse für alle Anwendungsfehler mit Message."""

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class PaymentDeclinedError(BaseError):
    pass


class CustomerNotFoundError(BaseError):
    pass


class ReserveError(BaseError):
    pass


class InventoryUnavailableError(BaseError):
    pass
