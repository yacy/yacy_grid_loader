/**
 *  LoaderListener
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

import org.json.JSONArray;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.loader.retrieval.ContentLoader;
import net.yacy.grid.mcp.AbstractBrokerListener;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.CronBox.Telemetry;
import net.yacy.grid.tools.Logger;
import net.yacy.grid.tools.Memory;

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
"cachePolicy": "if fresh",
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
public class LoaderListener extends AbstractBrokerListener implements BrokerListener {

    private final boolean disableHeadless;

    public LoaderListener(final YaCyServices service, final boolean disableHeadless) {
         super(Service.instance.config, service, Runtime.getRuntime().availableProcessors());
         this.disableHeadless = disableHeadless;
    }

    @Override
    public ActionResult processAction(final SusiAction action, final JSONArray data, final String processName, final int processNumber) {

        // check short memory status
        if (Memory.shortStatus()) {
            Logger.info(this.getClass(), "Loader short memory status: assigned = " + Memory.assigned() + ", used = " + Memory.used());
        }

        // find out if we should do headless loading
        final String crawlID = action.getStringAttr("id");
        if (crawlID == null || crawlID.length() == 0) {
            Logger.info(this.getClass(), "Loader.processAction Fail: Action does not have an id: " + action.toString());
            return ActionResult.FAIL_IRREVERSIBLE;
        }
        final JSONObject crawl = SusiThought.selectData(data, "id", crawlID);
        if (crawl == null) {
            Logger.info(this.getClass(), "Loader.processAction Fail: ID of Action not found in data: " + action.toString());
            return ActionResult.FAIL_IRREVERSIBLE;
        }
        final int depth = action.getIntAttr("depth");
        final int crawlingDepth = crawl.getInt("crawlingDepth");
        final int priority =  crawl.has("priority") ? crawl.getInt("priority") : 0;
        boolean loaderHeadless = crawl.has("loaderHeadless") ? crawl.getBoolean("loaderHeadless") : true;
        if (this.disableHeadless) loaderHeadless = false;

        final String targetasset = action.getStringAttr("targetasset");
        final String threadnameprefix = processName + "-" + processNumber;
        Thread.currentThread().setName(threadnameprefix + " targetasset=" + targetasset);
        if (targetasset != null && targetasset.length() > 0) {
            ActionResult actionResult = ActionResult.SUCCESS;
            final byte[] b;
            try {
                final ContentLoader cl = new ContentLoader(action, data, targetasset.endsWith(".gz"), threadnameprefix, crawlID, depth, crawlingDepth, loaderHeadless, priority);
                b = cl.getContent();
                actionResult = cl.getResult();
            } catch (final Throwable e) {
                Logger.warn(this.getClass(), e);
                return ActionResult.FAIL_IRREVERSIBLE;
            }
            if (actionResult == ActionResult.FAIL_IRREVERSIBLE) {
                Logger.info(this.getClass(), "Loader.processAction FAILED processed message for targetasset " + targetasset);
                return actionResult;
            }
            Logger.info(this.getClass(), "Loader.processAction SUCCESS processed message for targetasset " + targetasset);
            boolean storeToMessage = true; // debug version for now: always true TODO: set to false later
            try {
                Service.instance.config.gridStorage.store(targetasset, b);
                Logger.info(this.getClass(), "Loader.processAction stored asset " + targetasset);
            } catch (final Throwable e) {
                Logger.warn(this.getClass(), "Loader.processAction asset " + targetasset + " could not be stored, carrying the asset within the next action", e);
                storeToMessage = true;
            }
            if (storeToMessage) {
                final JSONArray actions = action.getEmbeddedActions();
                actions.forEach(a ->
                    new SusiAction((JSONObject) a).setBinaryAsset(targetasset, b)
                );
                Logger.info(this.getClass(), "Loader.processAction stored asset " + targetasset + " into message");
            }
            Logger.info(this.getClass(), "Loader.processAction processed message from queue and stored asset " + targetasset);

            // success (has done something)
            return actionResult;
        }

        // fail (nothing done)
        return ActionResult.FAIL_IRREVERSIBLE;
    }

    @Override
    public Telemetry getTelemetry() {
        return null;
    }
}