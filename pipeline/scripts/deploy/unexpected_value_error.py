class UnexpectedValueError(Exception):
    """Exception raised when we hit unexpected values.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
