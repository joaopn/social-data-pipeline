"""Errors raised when a request is refused at the control boundary.

Every error carries a short stable `code` for the client and a message for a person. A message names the verb and the field only and never holds a field's value: a value can be a secret, or text someone wrote so that it ends up in a log or on a page.
"""


class ControlError(Exception):
    """Base class: a request that was refused before anything acted on it."""

    code = "control_error"

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class UnknownVerb(ControlError):
    """The requested name is not a verb in the registry."""

    code = "unknown_verb"


class VerbNotAvailable(ControlError):
    """The verb exists but is not accepted over the socket."""

    code = "not_available"


class InputError(ControlError):
    """The fields of a request were refused. `field` is the name of the field at fault, or None when the fault is not one named field."""

    code = "bad_input"

    def __init__(self, message, field=None):
        super().__init__(message)
        self.field = field
