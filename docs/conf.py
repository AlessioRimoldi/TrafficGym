import sys
from pathlib import Path

# src/ must be on the path so `import trafficgym.engine.*` resolves correctly.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

project = "TrafficGym"
copyright = "2026, Diego Van Overberghe, Alessio Rimoldi"
author = "Diego Van Overberghe, Alessio Rimoldi"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
]

# Prevent autodoc from trying to import simulation/Django deps at doc-build time.
autodoc_mock_imports = ["libsumo", "django", "celery", "redis"]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "special-members": "__init__",
}

# Render type hints in the parameter descriptions, not in the signature.
typehints_fully_qualified = False
always_document_param_types = False
typehints_use_rtype = True

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_param = True
napoleon_use_rtype = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]

html_theme_options = {
    "source_repository": "https://github.com/AlessioRimoldi/TrafficGym",
    "source_branch": "main",
    "source_directory": "docs/",
}
