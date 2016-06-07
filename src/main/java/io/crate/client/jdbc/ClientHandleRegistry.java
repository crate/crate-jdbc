/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc;

import io.crate.client.CrateClient;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

class ClientHandleRegistry {

    private final ConcurrentHashMap<String, ClientHandle> clientHandles = new ConcurrentHashMap<>();

    public ClientHandleRegistry() {
    }

    Collection<String> urls() {
        return clientHandles.keySet();
    }

    ClientHandle getHandle(String url) {
        ClientHandle handle;
        synchronized (clientHandles) {
            handle = clientHandles.get(url);
            if (handle == null) {
                handle = new ClientHandle(url);
                clientHandles.put(url, handle);
            } else {
                handle.incRef();
            }
        }
        return handle;
    }

    public class ClientHandle {

        private int refCount;
        private final CrateClient client;
        private final String url;

        ClientHandle(String url) {
            refCount = 1;
            this.url = url;
            if (url.equals("/")) {
                client = new CrateClient();
            } else {
                String[] urlParts = url.split("/");
                String hosts = urlParts[0];
                client = new CrateClient(hosts.split(","));
            }
        }

        public CrateClient client() {
            return client;
        }

        public String url() {
            return url;
        }

        void connectionClosed() {
            synchronized (clientHandles) {
                if (--refCount == 0) {
                    client().close();
                    clientHandles.remove(this.url);
                }
            }
        }

        void incRef() {
            assert refCount > 0;
            refCount++;
        }

    }
}
