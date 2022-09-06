/**
 *  Loader
 *  Copyright 25.04.2017 by Michael Peter Christen, @0rb1t3r
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program in the file lgpl21.txt
 *  If not, see <http://www.gnu.org/licenses/>.
 */

package net.yacy.grid.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.Servlet;

import net.yacy.grid.YaCyServices;
import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;
import net.yacy.grid.loader.api.LoaderService;
import net.yacy.grid.loader.api.ProcessService;
import net.yacy.grid.loader.retrieval.LoaderClientConnection;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Configuration;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.CronBox;
import net.yacy.grid.tools.CronBox.Telemetry;
import net.yacy.grid.tools.Logger;

/**
 * The Loader main class
 *
 * performance debugging:
 * http://localhost:8200/yacy/grid/mcp/info/threaddump.txt
 * http://localhost:8200/yacy/grid/mcp/info/threaddump.txt?count=100 *
 */
public class Loader {

    private final static YaCyServices LOADER_SERVICE = YaCyServices.loader; // check with http://localhost:8200/yacy/grid/mcp/status.json
    private final static String DATA_PATH = "data";

    // define services
    @SuppressWarnings("unchecked")
    public final static Class<? extends Servlet>[] LOADER_SERVICES = new Class[]{
            // app services
            LoaderService.class,
            ProcessService.class
    };

    public static class Application implements CronBox.Application {

        final Configuration config;
        final Service service;
        final BrokerListener brokerApplication;
        final CronBox.Application serviceApplication;

        public Application() {
            Logger.info("Starting Crawler Application...");

            // initialize configuration
            final List<Class<? extends Servlet>> services = new ArrayList<>();
            services.addAll(Arrays.asList(MCP.MCP_SERVLETS));
            services.addAll(Arrays.asList(LOADER_SERVICES));
            this.config =  new Configuration(DATA_PATH, true, LOADER_SERVICE, services.toArray(new Class[services.size()]));

            // initialize loader with user agent
            String userAgent = ClientIdentification.getAgent(ClientIdentification.googleAgentName/*.yacyInternetCrawlerAgentName*/).userAgent;
            String userAgentType = this.config.properties.get("grid.loader.userAgentType");
            if (userAgentType == null || userAgentType.length() == 0) userAgentType = "BROWSER";
            if ("CUSTOM".equals(userAgentType)) userAgent = this.config.properties.get("grid.lodeer.userAgentName");
            else if ("YACY".equals(userAgentType)) userAgent = ClientIdentification.yacyInternetCrawlerAgent.userAgent;
            else if ("GOOGLE".equals(userAgentType)) userAgent = ClientIdentification.getAgent(ClientIdentification.googleAgentName).userAgent;
            else userAgent = ClientIdentification.getAgent(ClientIdentification.browserAgentName).userAgent;
            LoaderClientConnection.httpClient = ClientConnection.getClosableHttpClient(userAgent);

            // initialize REST server with services
            this.service = new Service(this.config);

            // connect backend
            this.config.connectBackend();

            // initiate broker application: listening to indexing requests at RabbitMQ
            final boolean disableHeadless = this.config.properties.containsKey("grid.loader.disableHeadless") ? Boolean.parseBoolean(this.config.properties.get("grid.loader.disableHeadless")) : false;
            this.brokerApplication = new LoaderListener(LOADER_SERVICE, disableHeadless);

            // initiate service application: listening to REST request
            this.serviceApplication = this.service.newServer(null);
        }

        @Override
        public void run() {

            Logger.info("Grid Name: " + this.config.properties.get("grid.name"));

            // starting threads
            new Thread(this.brokerApplication).start();
            this.serviceApplication.run(); // SIC! the service application is running as the core element of this run() process. If we run it concurrently, this runnable will be "dead".
        }

        @Override
        public void stop() {
            Logger.info("Stopping MCP Application...");
            this.serviceApplication.stop();
            this.brokerApplication.stop();
            this.service.stop();
            this.service.close();
            this.config.close();
        }

        @Override
        public Telemetry getTelemetry() {
            return null;
        }

    }

    public static void main(final String[] args) {
        // run in headless mode
        System.setProperty("java.awt.headless", "true"); // no awt used here so we can switch off that stuff

        // Debug Info
        boolean assertionenabled = false;
        assert (assertionenabled = true) == true; // compare to true to remove warning: "Possible accidental assignement"
        if (assertionenabled) Logger.info("Asserts are enabled");

        // first greeting
        Logger.info("YaCy Grid Loader started!");

        // run application with cron
        final long cycleDelay = Long.parseLong(System.getProperty("YACYGRID_LOADER_CYCLEDELAY", "" + Long.MAX_VALUE)); // by default, run only in one genesis thread
        final int cycleRandom = Integer.parseInt(System.getProperty("YACYGRID_LOADER_CYCLERANDOM", "" + 1000 * 60 /*1 minute*/));
        final CronBox cron = new CronBox(Application.class, cycleDelay, cycleRandom);
        cron.cycle();

        // this line is reached if the cron process was shut down
        Logger.info("YaCy Grid Loader terminated");
    }

}
