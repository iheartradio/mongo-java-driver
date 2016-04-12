/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package com.mongodb.event;

import com.mongodb.connection.ServerId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A connection pool-related event.
 *
 * @since 3.3
 */
public class ConnectionPoolEvent extends ClusterEvent {
    private final ServerId serverId;

    /**
     * Constructs a new instance of the event.
     *
     * @param serverId the server id
     */

    public ConnectionPoolEvent(final ServerId serverId) {
        super(serverId.getClusterId());
        this.serverId = notNull("serverId", serverId);
    }

    /**
     * Gets the server address associated with this connection pool
     *
     * @return the server address
     */
    public ServerId getServerId() {
        return serverId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ConnectionPoolEvent that = (ConnectionPoolEvent) o;

        if (!serverId.equals(that.serverId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + serverId.hashCode();
        return result;
    }
}
