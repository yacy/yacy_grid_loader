/**
 *  HtmlUnitLoader
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

package net.yacy.grid.loader.retrieval;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.TopLevelWindow;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebClientOptions;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.WebWindow;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.util.UrlUtils;

import net.yacy.grid.mcp.Data;
import net.yacy.grid.tools.Memory;

/**
 * http://htmlunit.sourceforge.net/
 */
public class HtmlUnitLoader {

    private static WebClient clientx = null;
    private static AtomicLong webClientUsage = new AtomicLong(1);
    static {
        initClient();
    }
    
    private static void initClient() {
        if (clientx != null) {
            clientx.getCache().clear();
        	clientx.close();
        }
        clientx = new WebClient(BrowserVersion.CHROME);
        WebClientOptions options = clientx.getOptions();
        options.setJavaScriptEnabled(true);
        options.setCssEnabled(false);
        options.setPopupBlockerEnabled(true);
        options.setRedirectEnabled(true);
        options.setThrowExceptionOnScriptError(false);
        clientx.getCache().setMaxSize(10000); // this might be a bit large, is regulated with throttling and client cache clear in short memory status
    }
    
    public static WebClient getClient() {
    	if (Memory.shortStatus() || webClientUsage.incrementAndGet() % 1000 == 0) {
    		initClient();
    	}
    	return clientx;
    }
    
    private String url, xml;

    public String getUrl() {
        return this.url;
    }

    public String getXml() {
        return this.xml;
    }

    public HtmlUnitLoader(String url, String windowName) throws IOException {// check short memory status
        
        WebClient client = getClient();
        this.url = url;
        HtmlPage page;
        try {
            long mem0 = Memory.available();
            URL uurl = UrlUtils.toUrlUnsafe(url);
            String htmlAcceptHeader = client.getBrowserVersion().getHtmlAcceptHeader();
            WebWindow webWindow = client.openWindow(uurl, windowName); // throws ClassCastException: com.gargoylesoftware.htmlunit.UnexpectedPage cannot be cast to com.gargoylesoftware.htmlunit.html.HtmlPage
            WebRequest webRequest = new WebRequest(uurl, htmlAcceptHeader);
            page = client.getPage(webWindow, webRequest); // com.gargoylesoftware.htmlunit.xml.XmlPage cannot be cast to com.gargoylesoftware.htmlunit.html.HtmlPage
            this.xml = page.asXml();
            webWindow.getEnclosedPage().cleanUp();
            if (webWindow instanceof TopLevelWindow) ((TopLevelWindow) webWindow).close();
            long mem1 = Memory.available();
            if (Memory.shortStatus()) client.getCache().clear();
            if (Memory.shortStatus()) {
                client.close();
                initClient();
            }
            Data.logger.info("HtmlUnitLoader loaded " + url + " - " + this.xml.length() + " bytes; used " + (mem1 - mem0) + " bytes");
        } catch (Throwable e) {
            // there can be many reasons here, i.e. an error in javascript
            // we should always treat this as if the error is within the HTMLUnit, not the web page.
            // Therefore, we should do a fail-over without HTMLUnit
            Data.logger.warn("HtmlUnitLoader Error loading " + url, e);
            
            // load the page with standard client anyway
            // to do this, we throw an IOException here and the caller must handle this
            throw new IOException(e.getMessage());
        }
    }

}
