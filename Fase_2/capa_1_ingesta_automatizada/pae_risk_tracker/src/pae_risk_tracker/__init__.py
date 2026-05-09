from __future__ import annotations

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)  # type: ignore[name-defined]

__all__ = ["__version__"]

__version__ = "0.1.0"
