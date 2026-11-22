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

# The guides' example and local addresses are not reachable from CI.
linkcheck_ignore = [r"http://localhost.*", r"https://localhost.*", r"https://api\.example.*"]
