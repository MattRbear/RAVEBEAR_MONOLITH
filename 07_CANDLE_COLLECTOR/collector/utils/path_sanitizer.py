"""
Path sanitization utilities for secure filesystem operations.
Prevents directory traversal and other path-based attacks.
"""
import re
from pathlib import Path


class PathSanitizationError(ValueError):
    """Raised when path sanitization fails."""
    pass


def sanitize_path_component(component: str, max_length: int = 64) -> str:
    """
    Sanitize a single path component (venue, symbol, timeframe).
    
    Rules:
    - Only alphanumeric, dash, underscore, slash allowed
    - No ".." sequences
    - No absolute paths (/, \, C:, etc.)
    - Max length enforced
    - No unicode trickery
    
    Args:
        component: Path component to sanitize
        max_length: Maximum allowed length
        
    Returns:
        Sanitized component
        
    Raises:
        PathSanitizationError: If component is invalid
    """
    if not component:
        raise PathSanitizationError("Path component cannot be empty")
    
    if len(component) > max_length:
        raise PathSanitizationError(
            f"Path component too long: {len(component)} > {max_length}"
        )
    
    # Check for directory traversal
    if ".." in component:
        raise PathSanitizationError(
            f"Directory traversal detected: {component}"
        )
    
    # Check for absolute paths
    if component.startswith("/") or component.startswith("\\"):
        raise PathSanitizationError(
            f"Absolute path not allowed: {component}"
        )
    
    # Check for drive letters (Windows)
    if re.match(r"^[A-Za-z]:", component):
        raise PathSanitizationError(
            f"Drive letter not allowed: {component}"
        )
    
    # Allowlist: alphanumeric, dash, underscore, slash, equals (for Hive partitioning)
    if not re.match(r"^[a-zA-Z0-9/_=.-]+$", component):
        raise PathSanitizationError(
            f"Invalid characters in path component: {component}"
        )
    
    return component


def sanitize_and_resolve(base_dir: Path, *components: str) -> Path:
    """
    Sanitize path components and resolve to absolute path.
    Ensures result stays within base_dir.
    
    Args:
        base_dir: Base directory (must be absolute)
        *components: Path components to sanitize and join
        
    Returns:
        Resolved absolute path
        
    Raises:
        PathSanitizationError: If path escapes base_dir or is invalid
    """
    if not base_dir.is_absolute():
        raise PathSanitizationError(
            f"Base directory must be absolute: {base_dir}"
        )
    
    # Sanitize each component
    sanitized = [sanitize_path_component(c) for c in components]
    
    # Build path
    result = base_dir
    for component in sanitized:
        result = result / component
    
    # Resolve to absolute path
    result = result.resolve()
    base_dir = base_dir.resolve()
    
    # Ensure result is within base_dir
    try:
        result.relative_to(base_dir)
    except ValueError:
        raise PathSanitizationError(
            f"Path escapes base directory: {result} not in {base_dir}"
        )
    
    return result
