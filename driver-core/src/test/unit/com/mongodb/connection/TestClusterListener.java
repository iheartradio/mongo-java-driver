/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterEvent;
import com.mongodb.event.ClusterListener;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class TestClusterListener implements ClusterListener {
    private ClusterEvent clusterOpeningEvent;
    private ClusterEvent clusterClosingEvent;
    private final List<ClusterDescriptionChangedEvent> clusterDescriptionChangedEvents = new ArrayList<ClusterDescriptionChangedEvent>();

    @Override
    public void clusterOpening(final ClusterEvent event) {
        isTrue("clusterOpeningEvent is null", clusterOpeningEvent == null);
        clusterOpeningEvent = event;
    }

    @Override
    public void clusterClosed(final ClusterEvent event) {
        isTrue("clusterClosingEvent is null", clusterClosingEvent == null);
        clusterClosingEvent = event;
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        notNull("event", event);
        clusterDescriptionChangedEvents.add(event);
    }

    public ClusterEvent getClusterOpeningEvent() {
        return clusterOpeningEvent;
    }

    public ClusterEvent getClusterClosingEvent() {
        return clusterClosingEvent;
    }

    public List<ClusterDescriptionChangedEvent> getClusterDescriptionChangedEvents() {
        return clusterDescriptionChangedEvents;
    }
}
