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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.TimeZone;

import com.gargoylesoftware.css.parser.CSSErrorHandler;
import com.gargoylesoftware.css.parser.CSSException;
import com.gargoylesoftware.css.parser.CSSParseException;
import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.BrowserVersion.BrowserVersionBuilder;
import com.gargoylesoftware.htmlunit.IncorrectnessListener;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.ScriptException;
import com.gargoylesoftware.htmlunit.TopLevelWindow;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebClientOptions;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.WebWindow;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.parser.HTMLParserListener;
import com.gargoylesoftware.htmlunit.javascript.JavaScriptErrorListener;
import com.gargoylesoftware.htmlunit.util.UrlUtils;

import net.yacy.grid.tools.Logger;
import net.yacy.grid.tools.Memory;

/**
 * http://htmlunit.sourceforge.net/
 */
public class HtmlUnitLoader {

    public static WebClient getClient() {
        return getClient(BrowserVersion.CHROME.getUserAgent());
    }

    public static WebClient getClient(String userAgent) {
        WebClient webClient = new WebClient(getBrowser(userAgent));
        WebClientOptions options = webClient.getOptions();
        options.setJavaScriptEnabled(true);
        options.setCssEnabled(false);
        options.setPopupBlockerEnabled(true);
        options.setRedirectEnabled(true);
        options.setDownloadImages(false);
        options.setGeolocationEnabled(false);
        options.setPrintContentOnFailingStatusCode(false);
        options.setThrowExceptionOnScriptError(false);
        options.setMaxInMemory(0);
        options.setHistoryPageCacheLimit(0);
        options.setHistorySizeLimit(0);
        //ProxyConfig proxyConfig = new ProxyConfig();
        //proxyConfig.setProxyHost("127.0.0.1");
        //proxyConfig.setProxyPort(Service.getPort());
        //options.setProxyConfig(proxyConfig);
        webClient.getCache().setMaxSize(10000); // this might be a bit large, is regulated with throttling and client cache clear in short memory status
        webClient.setIncorrectnessListener(new IncorrectnessListener() {
            @Override
            public void notify(String arg0, Object arg1) {}
        });
        webClient.setCssErrorHandler(new CSSErrorHandler() {
            @Override
            public void warning(CSSParseException exception) throws CSSException {}
            @Override
            public void error(CSSParseException exception) throws CSSException {}
            @Override
            public void fatalError(CSSParseException exception) throws CSSException {}
        });
        webClient.setJavaScriptErrorListener(new JavaScriptErrorListener() {
            @Override
            public void timeoutError(HtmlPage arg0, long arg1, long arg2) {}
            @Override
            public void scriptException(HtmlPage arg0, ScriptException arg1) {}
            @Override
            public void malformedScriptURL(HtmlPage arg0, String arg1, MalformedURLException arg2) {}
            @Override
            public void loadScriptError(HtmlPage arg0, URL arg1, Exception arg2) {}
            @Override
            public void warn(String message, String sourceName, int line, String lineSource, int lineOffset) {}
        });
        webClient.setHTMLParserListener(new HTMLParserListener() {
            @Override
            public void error(String message, URL url, String html, int line, int column, String key) {}
            @Override
            public void warning(String message, URL url, String html, int line, int column, String key) {}
        });
        return webClient;
    }


    private static BrowserVersion getBrowser(String userAgent) {
        BrowserVersionBuilder browserBuilder = getBrowserBuilder();
        browserBuilder.setUserAgent(userAgent);
        return browserBuilder.build();
    }

    private static BrowserVersionBuilder getBrowserBuilder() {
        BrowserVersionBuilder browserBuilder = new BrowserVersion.BrowserVersionBuilder(BrowserVersion.CHROME);
        browserBuilder.setSystemTimezone(TimeZone.getDefault());
        return browserBuilder;
    }

    private String url, xml;

    public String getUrl() {
        return this.url;
    }

    public String getXml() {
        return this.xml;
    }

    public HtmlUnitLoader(String url, String windowName) throws IOException {// check short memory status

        this.url = url;
        HtmlPage page;
        try (WebClient client = getClient()) {
            long mem0 = Memory.available();
            URL uurl = UrlUtils.toUrlUnsafe(url);
            String htmlAcceptHeader = client.getBrowserVersion().getHtmlAcceptHeader();
            WebWindow webWindow = client.openWindow(uurl, windowName); // throws ClassCastException: com.gargoylesoftware.htmlunit.UnexpectedPage cannot be cast to com.gargoylesoftware.htmlunit.html.HtmlPage
            WebRequest webRequest = new WebRequest(uurl, htmlAcceptHeader, null);
            page = client.getPage(webWindow, webRequest); // com.gargoylesoftware.htmlunit.xml.XmlPage cannot be cast to com.gargoylesoftware.htmlunit.html.HtmlPage
            this.xml = page.asXml();
            long mem1 = Memory.available();
            Page htmlpage = webWindow.getEnclosedPage();
            htmlpage.cleanUp();
            if (webWindow instanceof TopLevelWindow) ((TopLevelWindow) webWindow).close();
            for (WebWindow ww: client.getWebWindows()) {
                if (ww instanceof TopLevelWindow) ((TopLevelWindow) ww).close();
                ww.getJobManager().removeAllJobs();
            }
            client.deregisterWebWindow(webWindow);
            client.getCache().clear();
            client.close();
            long mem2 = Memory.available();
            Logger.info(this.getClass(), "HtmlUnitLoader loaded " + url + " - " + this.xml.length() + " bytes; used " + (mem1 - mem0) + " bytes, after cleanup " + (mem2 - mem0) + " bytes");
        } catch (Throwable e) {
            // there can be many reasons here, i.e. an error in javascript
            // we should always treat this as if the error is within the HTMLUnit, not the web page.
            // Therefore, we should do a fail-over without HTMLUnit
            // Data.logger.warn("HtmlUnitLoader Error loading " + url, e);
            // load the page with standard client anyway
            // to do this, we throw an IOException here and the caller must handle this
            throw new IOException(e.getMessage());
        }
    }

}
