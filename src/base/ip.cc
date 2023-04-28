/**
 * Copyright (c) 2023 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/ip.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_split.h"
#include "base/glog_wrapper.h"

namespace openmldb {
namespace base {

absl::StatusOr<std::string> Resolve(const std::string& name) {
    std::vector<std::string> domain_port = absl::StrSplit(name, ':');
    if (domain_port.size() != 2) {
        return absl::InternalError(absl::StrCat(name, " is not a valid pattern as 'ip:port' or 'domain:port'", name));
    }
    auto res = ResolveToIP(domain_port[0], openmldb::base::ResolveOpt::INET);
    if (!res.ok()) {
        return res.status();
    }
    if (res.value().empty()) {
        return absl::InternalError(absl::StrCat("fail to resolve endpoint, result list empty"));
    }

    // optimize later, here just take first entry simply work
    return absl::StrCat(res.value()[0], ":", domain_port[1]);
}

bool GetLocalIp(std::string *ip) {
    if (ip == nullptr) {
        return false;
    }
    char name[256];
    if (-1 == gethostname(name, sizeof(name))) {
        PDLOG(ERROR, "error getting hostname: %s", strerror(errno));
        return false;
    }
    struct hostent *host = gethostbyname(name);
    if (host == NULL) {
        PDLOG(ERROR, "error resolving hostname %s: %s", name, hstrerror(h_errno));
        return false;
    }
    char ip_str[32];
    const char *ret = inet_ntop(host->h_addrtype, host->h_addr_list[0], ip_str, sizeof(ip_str));
    if (ret == NULL) {
        PDLOG(ERROR, "error transforming IP to string: %s", strerror(errno));
        return false;
    }
    *ip = ip_str;
    return true;
}

absl::StatusOr<std::vector<std::string>> ResolveToIP(const std::string& name, ResolveOpt opt) {
    struct addrinfo hints, *res;
    int status;
    char ipstr[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    switch (opt) {
        case ResolveOpt::UNSPEC: {
            hints.ai_family = AF_UNSPEC;  // AF_INET or AF_INET6 to force version
            break;
        }
        case ResolveOpt::INET: {
            hints.ai_family = AF_INET;
            break;
        }
        case ResolveOpt::INET6: {
            hints.ai_family = AF_INET6;
            break;
        }
    }
    hints.ai_socktype = SOCK_STREAM;

    // getaddrinfo is preferred to gethostbyname
    if ((status = getaddrinfo(name.c_str(), NULL, &hints, &res)) != 0) {
        return absl::InternalError(absl::StrCat("getaddrinfo error: ", gai_strerror(status)));
    }
    absl::Cleanup clean = [&res] {
        freeaddrinfo(res);  // free the linked list
    };

    std::vector<std::string> out;
    for (struct addrinfo *p = res; p != NULL; p = p->ai_next) {
        void *addr;

        // get the pointer to the address itself,
        // different fields in IPv4 and IPv6:
        if (p->ai_family == AF_INET) {
            // IPv4
            struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
            addr = &(ipv4->sin_addr);
        } else {  // IPv6
            struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)p->ai_addr;
            addr = &(ipv6->sin6_addr);
        }

        auto* ret = inet_ntop(p->ai_family, addr, ipstr, sizeof ipstr);
        if (ret == NULL) {
            return absl::InternalError(absl::StrCat("error transforming IP to string: ", strerror(errno)));
        }
        out.emplace_back(ipstr);
    }

    return out;
}
}  // namespace base
}  // namespace openmldb
