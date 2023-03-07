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

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServletResponse;

import io.prometheus.client.exporter.common.TextFormat;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller("prometheusController")
public class PrometheusController {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public PrometheusController() {
        log.error("There was a controller");
        log.warn("There was a controller");
        log.info("There was a controller");
        log.debug("There was a controller");
    }

    @ApiOperation(value = "Supply metrics for Prometheus")
    @RequestMapping(value = "/metrics", method = RequestMethod.GET)
    protected void doGet(final HttpServletResponse resp)
            throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(TextFormat.CONTENT_TYPE_004);
        try (Writer writer = resp.getWriter()) {
            PrometheusNotifier.registryFactory.getRegistry().scrape(writer);
            writer.flush();
        }
    }
}