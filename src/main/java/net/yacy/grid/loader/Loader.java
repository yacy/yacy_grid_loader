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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.Servlet;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.io.assets.StorageFactory;
import net.yacy.grid.io.messages.MessageContainer;
import net.yacy.grid.loader.api.LoaderService;
import net.yacy.grid.loader.api.ProcessService;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;

public class Loader {

    private final static YaCyServices SERVICE = YaCyServices.loader;
    private final static String DATA_PATH = "data";
    private final static String APP_PATH = "loader";
 
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
  "data": [{"collection": "test"}],
  "actions": [{
    "type": "loader",
    "queue": "webloader",
    "urls": ["http://yacy.net"],
    "collection": "test",
    "targetasset": "test3/yacy.net.warc.gz"
  },{
    "type": "parser",
    "queue": "yacyparser",
    "sourceasset": "test3/yacy.net.warc.gz",
    "targetasset": "test3/yacy.net.jsonlist"
  },{
    "type": "indexer",
    "queue": "elasticsearch",
    "sourceasset": "test3/yacy.net.jsonlist"
  },
  ]
}
     */
    public static class BrokerListener extends Thread {
        public boolean shallRun = true;
        
        @Override
        public void run() {
            while (shallRun) {
                if (Data.gridBroker == null) {
                    try {Thread.sleep(1000);} catch (InterruptedException ee) {}
                } else try {
                    MessageContainer<byte[]> mc = Data.gridBroker.receive(YaCyServices.loader.name(), "webloader", 10000);
                    if (mc == null || mc.getPayload() == null) continue;
                    JSONObject json = new JSONObject(new JSONTokener(new String(mc.getPayload(), StandardCharsets.UTF_8)));
                    SusiThought process = new SusiThought(json);
                    List<SusiAction> actions = process.getActions();
                    if (!actions.isEmpty()) {
                        SusiAction a = actions.get(0);
                        String targetasset = a.getStringAttr("targetasset");
                        if (targetasset != null && targetasset.length() > 0) {
                            byte[] b = HttpLoader.eval(process, targetasset.endsWith(".gz"));
                            try {
                                Data.gridStorage.store(targetasset, b);
                                Data.logger.info("processed message from queue and stored asset " + targetasset);
                                
                                // send next action to queue
                                actions.remove(0);
                                if (actions.size() > 0) {
                                    a = actions.get(0);
                                    String type = a.getStringAttr("type");
                                    String queue = a.getStringAttr("queue");
                                    if (type.length() > 0 && queue.length() > 0) {
                                        // create a new Thought and push it to the next queue
                                        JSONArray nextActions = new JSONArray();
                                        actions.forEach(action -> nextActions.put(action.toJSONClone()));
                                        JSONObject nextProcess = new JSONObject().put("data", process.getData()).put("actions", nextActions);
                                        Data.gridBroker.send(type, queue, nextProcess.toString().getBytes(StandardCharsets.UTF_8));
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    try {Thread.sleep(1000);} catch (InterruptedException ee) {}
                }
            }
        }
        public void terminate() {
            this.shallRun = false;
        }
    }
    
    public static void main(String[] args) {
        BrokerListener brokerListener = new BrokerListener();
        brokerListener.start();
        List<Class<? extends Servlet>> services = new ArrayList<>();
        services.addAll(Arrays.asList(MCP.MCP_SERVICES));
        services.addAll(Arrays.asList(LOADER_SERVICES));
        Service.runService(SERVICE, DATA_PATH, APP_PATH, services);
        brokerListener.terminate();
    }
    
}
