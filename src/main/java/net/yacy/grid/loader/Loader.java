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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.Servlet;

import org.json.JSONArray;

import ai.susi.mind.SusiAction;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.loader.api.LoaderService;
import net.yacy.grid.loader.api.ProcessService;
import net.yacy.grid.mcp.AbstractBrokerListener;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;

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
    
    /**
     * broker listener, takes process messages from the queue "loader", "webloader"
     * i.e. test with:
     * curl -X POST -F "message=@job.json" -F "serviceName=loader" -F "queueName=webloader" http://yacygrid.com:8100/yacy/grid/mcp/messages/send.json
     * where job.json is:
{
  "metadata": {
    "process": "yacy_grid_loader",
    "count": 1
  },
  "data": [{
    "crawlingMode": "url",
    "crawlingURL": "http://yacy.net",
    "sitemapURL": "",
    "crawlingFile": "",
    "crawlingDepth": 3,
    "crawlingDepthExtension": "",
    "range": "domain",
    "mustmatch": ".*",
    "mustnotmatch": "",
    "ipMustmatch": ".*",
    "ipMustnotmatch": "",
    "indexmustmatch": ".*",
    "indexmustnotmatch": "",
    "deleteold": "off",
    "deleteIfOlderNumber": 0,
    "deleteIfOlderUnit": "day",
    "recrawl": "nodoubles",
    "reloadIfOlderNumber": 0,
    "reloadIfOlderUnit": "day",
    "crawlingDomMaxCheck": "off",
    "crawlingDomMaxPages": 1000,
    "crawlingQ": "off",
    "directDocByURL": "off",
    "storeHTCache": "off",
    "cachePolicy": "if fresh",
    "indexText": "on",
    "indexMedia": "off",
    "xsstopw": "off",
    "collection": "user",
    "agentName": "yacybot (yacy.net; crawler from yacygrid.com)",
    "user": "anonymous@nowhere.com",
    "client": "yacygrid.com"
  }],
  "actions": [{
    "type": "loader",
    "queue": "webloader",
    "urls": ["http://yacy.net"],
    "collection": "test",
    "targetasset": "test3/yacy.net.warc.gz",
    "actions": [{
      "type": "parser",
      "queue": "yacyparser",
      "sourceasset": "test3/yacy.net.warc.gz",
      "targetasset": "test3/yacy.net.jsonlist",
      "targetgraph": "test3/yacy.net.graph.json",
      "actions": [{
        "type": "indexer",
        "queue": "elasticsearch",
        "sourceasset": "test3/yacy.net.jsonlist"
      },{
        "type": "crawler",
        "queue": "webcrawler",
        "sourceasset": "test3/yacy.net.graph.json"
      }
      ]
    }]
  }]
}
     *
     * to check the queue content, see http://www.searchlab.eu:15672/
     */
    public static class LoaderListener extends AbstractBrokerListener implements BrokerListener {

        public LoaderListener(YaCyServices service) {
             super(service, Runtime.getRuntime().availableProcessors());
        }
        
        @Override
        public boolean processAction(SusiAction action, JSONArray data) {
            String targetasset = action.getStringAttr("targetasset");
            if (targetasset != null && targetasset.length() > 0) {
                byte[] b = HttpLoader.eval(action, data, targetasset.endsWith(".gz"));
                try {
                    Data.gridStorage.store(targetasset, b);
                    Data.logger.info("processed message from queue and stored asset " + targetasset);
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return false;
        }
    }
    
    public static void main(String[] args) {
        // initialize environment variables
        List<Class<? extends Servlet>> services = new ArrayList<>();
        services.addAll(Arrays.asList(MCP.MCP_SERVICES));
        services.addAll(Arrays.asList(LOADER_SERVICES));
        Service.initEnvironment(LOADER_SERVICE, services, DATA_PATH);

        // start listener
        BrokerListener brokerListener = new LoaderListener(LOADER_SERVICE);
        new Thread(brokerListener).start();
        
        // start server
        Service.runService(null);
        brokerListener.terminate();
    }
    
}
