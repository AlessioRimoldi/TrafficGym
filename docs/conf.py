import sys
from pathlib import Path

sys.path.insert(0, str(Path("..", "src", "trafficgym").resolve()))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'sumo_workbench_engine'
copyright = '2026, Diego Van Overberghe, Alessio Rimoldi'
author = 'Diego Van Overberghe, Alessio Rimoldi'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
  "sphinx.ext.autodoc",
  "sphinx.ext.napoleon",
  "sphinx_autodoc_typehints"
]

autodoc_default_options = {
  "members": True,
  "undoc-members": True,
  "show-inheritance": True,
}

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
