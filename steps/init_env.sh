#!/bin/bash
#
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

# init_env.sh

set -e

pushd /depends

mkdir -p thirdparty

tar xzf thirdparty.tar.gz -C thirdparty --strip-components=1

rm -rf thirdparty/hybridse
mkdir -p thirdparty/hybridse
curl -SLo hybridse-0.2.0-beta4-linux-x86_64.tar.gz https://github.com/aceforeverd/HybridSE/releases/download/v0.2.0-beta4/hybridse-0.2.0-beta4-linux-x86_64.tar.gz
tar xzf hybridse-*.tar.gz -C thirdparty/hybridse --strip-components=1

popd
ln -sf /depends/thirdparty thirdparty
ln -sf /depends/thirdsrc thirdsrc
