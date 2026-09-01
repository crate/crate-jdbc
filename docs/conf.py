from crate.theme.rtd.conf.jdbc import *

exclude_patterns = ['.crate-docs/**', 'requirements.txt']

# Enable version chooser.
html_context.update({
    "display_version": True,
})
