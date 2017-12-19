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

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

/**
 * http://htmlunit.sourceforge.net/
 */
public class HtmlUnitLoader {

    private static WebClient client;
    static {
        client = new WebClient(BrowserVersion.CHROME);
        client.getOptions().setJavaScriptEnabled(true);
        client.getCache().setMaxSize(1000); // this might be a bit large TODO: we must automatically scale here
    }

    private String url, xml;

    public String getUrl() {
        return this.url;
    }

    public String getXml() {
        return this.xml;
    }

    public HtmlUnitLoader(String url) throws IOException {
        this.url = url;
        HtmlPage page;
        try {
            page = client.getPage(url);
            this.xml = page.asXml();
        } catch (Throwable e) {
            // there can be many reasons here, i.e. an error in javascript
            // we should always treat this as if the error is within the HTMLUnit, not the web page.
            // Therefore, we should do a fail-over without HTMLUnit
            e.printStackTrace();
            
            // load the page with standard client anyway
            // to do this, we throw an IOException here and the caller must handle this
            throw new IOException(e.getMessage());
        }
    }

}
