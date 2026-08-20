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

# Tags the version the project is at and pushes the tag, which is what starts
# the release workflow. Run it from the repository root, or through `make tag`.
#
# A pushed tag cannot be taken back, so everything the release workflow would
# then refuse the tag for is checked here first.

set -euo pipefail

fail() {
    echo "$1" >&2
    echo "Aborting." >&2
    exit 1
}

[ -z "$(git status --porcelain)" ] || fail "Working directory not clean. Commit before tagging."

echo "Fetching origin..."
git fetch --quiet origin

BRANCH="$(git branch --show-current)"
[ -n "$BRANCH" ] || fail "HEAD is detached; check out the branch to release from."
echo "Current branch is $BRANCH."

if [ "$(git rev-parse "$BRANCH")" != "$(git rev-parse "origin/$BRANCH")" ]; then
    fail "Local $BRANCH is not up to date with origin/$BRANCH."
fi

VERSION="$(./gradlew -q getVersion | tr -d '[:space:]')"
[ -n "$VERSION" ] || fail "Could not read the project version."
echo "Version is $VERSION."

case "$VERSION" in
    *SNAPSHOT*) fail "Refusing to release the snapshot version $VERSION." ;;
esac

if git rev-parse -q --verify "refs/tags/$VERSION" > /dev/null; then
    fail "Revision $VERSION is already tagged."
fi

# The release workflow writes its GitHub release from these notes, and refuses
# a version that has none.
./devtools/release_notes.sh "$VERSION" > /dev/null || fail "CHANGES.txt does not document $VERSION."

echo "Creating tag $VERSION ..."
git tag -a "$VERSION" -m "Release $VERSION"
git push origin "refs/tags/$VERSION"
echo "Done."
