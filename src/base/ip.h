/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_BASE_IP_H_
#define SRC_BASE_IP_H_

#include <string>
#include <vector>
#include "absl/status/statusor.h"

namespace openmldb {
namespace base {

enum class ResolveOpt {
    UNSPEC,
    INET,
    INET6,
};

// resolve domain:port to ip:port if necessary
absl::StatusOr<std::string> Resolve(const std::string& name);

// resolve the {name} into string represent of IP address
absl::StatusOr<std::vector<std::string>> ResolveToIP(const std::string& name, ResolveOpt opt);

bool GetLocalIp(std::string *ip);

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_IP_H_
