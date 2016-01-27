/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import com.mongodb.annotations.Beta;
import com.mongodb.connection.ClusterId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A cluster-related event.
 */
@Beta
public class ClusterEvent {
    private final ClusterId clusterId;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     */
    public ClusterEvent(final ClusterId clusterId) {
        this.clusterId = notNull("clusterId", clusterId);
    }

    /**
     * Gets the cluster id associated with this event.
     *
     * @return the cluster id
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterEvent that = (ClusterEvent) o;

        if (!clusterId.equals(that.clusterId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return clusterId.hashCode();
    }
}

