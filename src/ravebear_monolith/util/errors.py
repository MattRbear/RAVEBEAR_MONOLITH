"""Custom exception types for Ravebear Monolith."""


class ConfigError(Exception):
    """Raised when configuration loading or validation fails."""

    pass


class OrchestratorError(Exception):
    """Raised when orchestrator encounters a fatal error."""

    pass
