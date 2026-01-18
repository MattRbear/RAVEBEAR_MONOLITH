"""Custom exception types for Ravebear Monolith."""


class ConfigError(Exception):
    """Raised when configuration loading or validation fails."""

    pass


class OrchestratorError(Exception):
    """Raised when orchestrator encounters a fatal error."""

    pass


class RateLimitError(Exception):
    """Raised when rate limit is exceeded or request exceeds burst capacity."""

    pass


class BudgetNotFoundError(Exception):
    """Raised when requesting an unknown budget bucket."""

    pass
