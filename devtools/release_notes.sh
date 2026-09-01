#!/bin/bash

# Licensed to Crate under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.  Crate licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

# Prints the CHANGES.txt notes for a version, and fails when it has none.
# The tagging script uses it to refuse an undocumented tag, and the release
# workflow both to gate the release and to write its GitHub release notes.
#
#   ./devtools/release_notes.sh 3.0.0

set -euo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    echo "usage: $0 <version>" >&2
    exit 2
fi

# The heading of a released version is dated: "2023/04/18 2.7.0". The notes
# run to the next such heading.
NOTES=$(awk -v version="$VERSION" '
    $0 ~ "^[0-9/]+ " version "$" { found = 1; next }
    found && $0 ~ "^=+$" { next }
    found && $0 ~ "^[0-9/]+ [0-9]+\\.[0-9]+\\.[0-9]+$" { exit }
    found { print }
' CHANGES.txt)

if [ -z "$(echo "$NOTES" | tr -d '[:space:]')" ]; then
    echo "CHANGES.txt has no dated heading (YYYY/MM/DD $VERSION) with notes under it" >&2
    exit 1
fi

echo "$NOTES"
