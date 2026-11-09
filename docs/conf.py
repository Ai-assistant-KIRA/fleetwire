"""Sphinx configuration for the fleetwire documentation (MyST Markdown)."""

project = "fleetwire"
author = "Reda Alaarabi"
copyright = "2026, Reda Alaarabi"

extensions = ["myst_parser", "sphinxcontrib.mermaid"]
source_suffix = {".md": "markdown"}
myst_enable_extensions = ["colon_fence", "deflist"]
myst_fence_as_directive = ["mermaid"]
myst_heading_anchors = 3

exclude_patterns = ["_build", "results/raw", "requirements.txt"]
html_theme = "furo"
html_title = "fleetwire"

# Links to source files and the OpenAPI document are relative to the
# repository, which the docs do not contain; check them in CI with the
# repository checked out instead.
linkcheck_ignore = [r"http://localhost.*", r"https://localhost.*", r"https://api\.example.*"]
suppress_warnings = ["myst.xref_missing"]
