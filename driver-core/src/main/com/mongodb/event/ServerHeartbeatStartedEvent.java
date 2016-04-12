/*
 * Copyright 2016 MongoDB, Inc.
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
 *
 */

package com.mongodb.event;

import com.mongodb.connection.ConnectionId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for the start of a server heartbeat.
 *
 * @since 3.3
 */
public class ServerHeartbeatStartedEvent extends ServerEvent {
    private final ConnectionId connectionId;

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connnectionId
     */
    public ServerHeartbeatStartedEvent(final ConnectionId connectionId) {
        super(connectionId.getServerId());
        this.connectionId = notNull("connectionId", connectionId);
    }

    /**
     * Gets the connectionId.
     *
     * @return the connectionId
     */
    public ConnectionId getConnectionId() {
        return connectionId;
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

        ServerHeartbeatStartedEvent that = (ServerHeartbeatStartedEvent) o;

        if (!connectionId.equals(that.connectionId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + connectionId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ServerHeartbeatStartedEvent{"
                + "connectionId=" + connectionId
                + "} " + super.toString();
    }
}
