class ExecutionRuntimeError(Exception):
    """Base exception for execution runtime errors."""
    pass

class ExecutionValidationError(ExecutionRuntimeError):
    """Raised when execution fails runtime validation."""
    
class DatasetNotSynchronizedError(ExecutionValidationError):
    """Raised when an execution references a dataset that is not synchronized."""