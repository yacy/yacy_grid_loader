/**
 *  LoaderService
 *  Copyright 25.4.2017 by Michael Peter Christen, @0rb1t3r
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

package net.yacy.grid.loader.api;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiAction.RenderType;
import ai.susi.mind.SusiThought;
import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;
import net.yacy.grid.loader.retrieval.ContentLoader;
import net.yacy.grid.mcp.Service;

/**
 *
 * Test URL:
 * http://localhost:8200/yacy/grid/loader/warcloader.warc.gz?url=http://yacy.net
 *
 * Test command:
 * curl "http://localhost:8200/yacy/grid/loader/warcprocess.json?collection=test&targetasset=test/yacy.net.warc.gz&url=http://yacy.net"
 * places the warc file on the asset store
 */
public class ProcessService extends ObjectAPIHandler implements APIHandler {

    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "warcprocess";

    @Override
    public String getAPIPath() {
        return "/yacy/grid/loader/" + NAME + ".json";
    }

    @Override
    public ServiceResponse serviceImpl(final Query call, final HttpServletResponse response) {
        // construct the same process as if it was submitted on a queue
        final SusiThought process = queryToProcess(call);
        final SusiAction action = process.getActions().iterator().next();
        final JSONArray data = process.getData();

        // find out if we should do headless loading
        final String crawlID = action.getStringAttr("id");
        final JSONObject crawl = SusiThought.selectData(data, "id", crawlID);
        final int depth = action.getIntAttr("depth");
        final int crawlingDepth = crawl.getInt("crawlingDepth");
        final int priority =  crawl.has("priority") ? crawl.getInt("priority") : 0;
        final boolean loaderHeadless = crawl.has("loaderHeadless") ? crawl.getBoolean("loaderHeadless") : true;

        // construct a WARC
        final String targetasset = process.getObservation("targetasset");
        final ContentLoader cl = new ContentLoader(
                process.getActions().get(0), process.getData(), targetasset.endsWith(".gz"), "api call from " + call.getClientHost(),
                crawlID, depth, crawlingDepth, loaderHeadless, priority);
        final byte[] b = cl.getContent();

        // store the WARC as asset if wanted
        final JSONObject json = new JSONObject(true);
        if (targetasset != null && targetasset.length() > 0) {
            try {
                Service.instance.config.gridStorage.store(targetasset, b);
                json.put(ObjectAPIHandler.SUCCESS_KEY, true);
                json.put(ObjectAPIHandler.COMMENT_KEY, "asset stored");
            } catch (final IOException e) {
                e.printStackTrace();
                json.put(ObjectAPIHandler.SUCCESS_KEY, false);
                json.put(ObjectAPIHandler.COMMENT_KEY, e.getMessage());
            }
        } else {
            json.put(ObjectAPIHandler.SUCCESS_KEY, false);
            json.put(ObjectAPIHandler.COMMENT_KEY, "this process requires a 'targetasset' attribute");
        }
        return new ServiceResponse(json);
    }

    public static SusiThought queryToProcess(final Query call) {
        // read query attributes
        final String id = call.get("id", "*id*"); // the crawl id
        String url = call.get("url", "");
        final int urlCount = 1;
        final int depth = call.get("depth", 0);
        final int crawlingDepth = call.get("crawlingDepth", 0); // the maximum depth for the crawl start of this domain
        final boolean loaderHeadless = call.get("loaderHeadless", false);
        final int priority = call.get("priority", 0);
        final String collection = call.get("collection", "");
        final String targetasset = call.get("targetasset", "");

        // construct an object that could be taken from the queue server
        final SusiThought process = new SusiThought();
        process.setProcess("yacy_grid_loader");
        if (collection.length() > 0) process.addObservation("collection", collection);

        final JSONObject crawl = new JSONObject();
        crawl.put("id", id);
        crawl.put("start_url", url);
        crawl.put("crawlingDepth", crawlingDepth);
        crawl.put("priority", priority);
        crawl.put("loaderHeadless", loaderHeadless);

        // create action
        final JSONObject action = new JSONObject();
        final JSONArray urls = new JSONArray();
        urls.put(url);
        action.put("id", id);
        action.put("type", RenderType.loader.name());
        action.put("queue", "loader");
        action.put("urls", urls);
        action.put("depth", depth);
        if (collection.length() > 0) action.put("collection", collection);
        if (targetasset.length() > 0) action.put("targetasset", targetasset);
        if (url.length() > 0) urls.put(url);
        if (urlCount > 0) for (int i = 0; i < urlCount; i++) {
            url = call.get("url_" + i, "");
            if (url.length() > 0) urls.put(url);
        }
        process.addAction(new SusiAction(action));
        process.setData(new JSONArray().put(crawl));

        return process;
    }

}

