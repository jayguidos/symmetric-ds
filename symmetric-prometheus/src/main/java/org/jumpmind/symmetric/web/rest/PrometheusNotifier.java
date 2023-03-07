/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.web.rest;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.micrometer.core.instrument.Counter;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.notification.INotificationType;

public class PrometheusNotifier
        implements INotificationType,
        ISymmetricEngineAware {
    static PrometheusMeterRegistryFactory registryFactory;
    private final ConcurrentMap<String, Counter> gauges = new ConcurrentHashMap<>();
    protected ISymmetricEngine engine;

    @Override
    public void notify(Notification notification, List<MonitorEvent> monitorEvents) {
        monitorEvents.forEach(e -> gauges.computeIfAbsent(
                e.getMonitorId() + "." + e.getType(),
                k -> Counter
                        .builder("sds.event.over.threshold")
                        .description("SDS monitored events that exceed their threshold")
                        .tags(
                                "srcNodeId", e.getNodeId(),
                                "eventType", e.getType())
                        .register(registryFactory.getRegistry()))
                .increment());
    }

    public void setRegistryFactory(PrometheusMeterRegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public String getName() {
        return "prometheus";
    }
}
