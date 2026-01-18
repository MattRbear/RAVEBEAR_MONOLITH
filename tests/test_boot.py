"""Boot test - verifies package can be imported and has version."""

import ravebear_monolith


def test_version_exists():
    """Verify __version__ is defined and non-empty."""
    assert hasattr(ravebear_monolith, "__version__")
    assert isinstance(ravebear_monolith.__version__, str)
    assert len(ravebear_monolith.__version__) > 0
