from crate.theme.rtd.conf.jdbc import *

exclude_patterns = ['.crate-docs/**', 'requirements.txt']

linkcheck_anchors_ignore = [
    r"diff-.*",
]

# Enable version chooser.
html_context.update({
    "display_version": True,
})
