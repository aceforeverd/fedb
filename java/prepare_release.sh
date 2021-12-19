#!/bin/bash
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# prepare for maven release, it tweak pom.xml based on a given version number
#  acceptable version style
#  - release version: '0.1.4'
#  - snapshot version: '0.1.4-SNAPSHOT'
#  - beta/alpha: '0.12.2.beta1', '0.12.2.alpha2', will convert released as '0.12.2-SNAPSHOT'
#  - a optional debug number come after base version, 0.12.2.33.alpha -> 0.12.2.33-SNAPSHOT
#  - string literal 'main': it is treated as a push to main, no version number will tweaked
#
# in case the version number passed in (extracted from tag) is in wrong style, the script will
# replace any string after 'x.x.x' with '-SNAPSHOT', to avoid publish directly into maven central

set -eE

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

if [ -z "$1" ]; then
    echo -e "Usage: $0 \$VERSION.\n\terror: version number required"
    exit 1
fi

cd "$(dirname "$0")"

VERSION=$1

if [[ $VERSION = 'main' ]]; then
  echo -e "${GREEN}quiting... not release from a tag push, $0 will not tweak the java versions but instead respect versions already set in code${NC}"
  exit 0;
fi
# rm semVer number from VERSION of 3 numbers MAJOR.MINOR.PATCH
#  0.1.2 -> ''
#  0.1.2-SNAPSHOT -> '-SNAPSHOT'
#  0.1.2.beta1    -> '.beta1'
#  0.2.1.0928     -> '.0928'
# shellcheck disable=SC2001
SUFFIX_VERSION=$(echo "$VERSION" | sed -e 's/^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*//')
# shellcheck disable=SC2001
DEBUG_SUFFIX_VERSION=$(echo "$SUFFIX_VERSION" | sed -e 's/\.[0-9][0-9]*//')
# get BASE VERSION by rm suffix version
if [[ "$DEBUG_SUFFIX_VERSION" != "$SUFFIX_VERSION" ]] ; then
  DEBUG_NO=${SUFFIX_VERSION%"$DEBUG_SUFFIX_VERSION"}
fi

BASE_VERSION=${VERSION%"$SUFFIX_VERSION"}
if [[ -n $SUFFIX_VERSION ]] ; then
    SUFFIX_VERSION=-SNAPSHOT
fi

if [[ -z $BASE_VERSION ]]; then
  echo -e "${RED}invalid version number inputed: $VERSION${NC}"
  exit 1
fi

# VERSION             -> BASE_VERSION    DEBUG_NO    SUFFIX_VERSION
# 0.1.2               -> 0.1.2
# 0.1.2-SNAPSHOT      -> 0.1.2                       SNAPSHOT
# 0.1.2.0928          -> 0.2.1           .0928       SNAPSHOT
# 0.1.2.0928ABC       -> 0.2.1           .0928       SNAPSHOT
# 0.1.2.0928-XXXXXXXX -> 0.2.1           .0928       SNAPSHOT
# 0.1.2.ABC           -> 0.2.1                       SNAPSHOT
# 0.1.2-0928          -> 0.2.1                       SNAPSHOT
echo "BASE_VERSION: ${BASE_VERSION}, DEBUG_NO: ${DEBUG_NO}, SUFFIX_VERSION: ${SUFFIX_VERSION}"
JAVA_VERSION="$BASE_VERSION${DEBUG_NO}$SUFFIX_VERSION"
echo -e "${GREEN}setting release version to: $JAVA_VERSION${NC}"

mvn versions:set -DnewVersion="$JAVA_VERSION"
