/**
 *  ContentLoader
 *  Copyright 11.5.2017 by Michael Peter Christen, @0rb1t3r
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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONArray;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

import com.gargoylesoftware.htmlunit.BrowserVersion;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiAction.RenderType;
import net.yacy.grid.io.index.CrawlerDocument;
import net.yacy.grid.io.index.CrawlerDocument.Status;
import net.yacy.grid.loader.JwatWarcWriter;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.tools.Classification.ContentDomain;
import net.yacy.grid.tools.Digest;
import net.yacy.grid.tools.Memory;
import net.yacy.grid.tools.MultiProtocolURL;

public class ContentLoader {
    
    
    public static byte[] eval(SusiAction action, JSONArray data, boolean compressed, String threadnameprefix) {
        // this must have a loader action
        if (action.getRenderType() != RenderType.loader) return new byte[0];
        
        // extract urls
        JSONArray urls = action.getArrayAttr("urls");
        List<String> urlss = new ArrayList<>();
        urls.forEach(u -> urlss.add(((String) u)));
        byte[] payload = data.toString(2).getBytes(StandardCharsets.UTF_8);
        return load(urlss, payload, compressed, threadnameprefix);
    }

    public static byte[] load(List<String> urls, byte[] header, boolean compressed, String threadnameprefix) {
        Thread.currentThread().setName(threadnameprefix + " loading " + urls.toString());
        
        // construct a WARC
        OutputStream out;
        File tmp = null;
        try {
            tmp = createTempFile("yacygridloader", ".warc");
            //Data.logger.info("creating temporary file: " + tmp.getAbsolutePath());
            out = new BufferedOutputStream(new FileOutputStream(tmp));
        } catch (IOException e) {
            tmp = null;
            out = new ByteArrayOutputStream();
        }
        try {
            WarcWriter ww = ContentLoader.initWriter(out, header, compressed);
            Map<String, String> errors = ContentLoader.load(ww, urls, threadnameprefix);
            errors.forEach((u, c) -> Data.logger.debug("Loader - cannot load: " + u + " - " + c));
        } catch (IOException e) {
            Data.logger.warn("ContentLoader.load cannot init WarcWriter", e);
        }
        if (out instanceof ByteArrayOutputStream) {
            byte[] b = ((ByteArrayOutputStream) out).toByteArray();
            return b;
        } else {
            try {
                out.close();
                // open the file again to create a byte[]
                byte[] b = Files.readAllBytes(tmp.toPath());
                tmp.delete();
                if (tmp.exists()) tmp.deleteOnExit();
                return b;
            } catch (IOException e) {
                // this should not happen since we had been able to open the file
                Data.logger.warn("", e);
                return new byte[0];
            }
        }
    }
    
    private final static SimpleDateFormat millisFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.US);
    private final static AtomicLong createTempFileCounter = new AtomicLong(0);
    public static File createTempFile(String prefix, String suffix) throws IOException {
        String tmpprefix = prefix + "-" + millisFormat.format(new Date()) + Long.toString(createTempFileCounter.getAndIncrement());
        File tmp = File.createTempFile(tmpprefix, suffix);
        return tmp;
    }
    
    private static WarcWriter initWriter(OutputStream out, byte[] payload, boolean compressed) throws IOException {
        WarcWriter ww = WarcWriterFactory.getWriter(out, compressed);
        JwatWarcWriter.writeWarcinfo(ww, new Date(), null, null, payload);
        return ww;
    }
    
    public static Map<String, String> load(final WarcWriter warcWriter, final List<String> urls, final String threadName) {
        Map<String, String> errors = new LinkedHashMap<>();
        urls.forEach(url -> {
            try {
                load(warcWriter, url, threadName);
            } catch (Throwable e) {
                Data.logger.warn("ContentLoader cannot load " + url + " - " + e.getMessage());
                errors.put((String) url, e.getMessage());
            }
        });
        return errors;
    }

    public static void load(final WarcWriter warcWriter, String url, final String threadName) throws IOException {
        if (url.indexOf("//") < 0) url = "http://" + url;
        

        // load entry from crawler index
        String urlid = Digest.encodeMD5Hex(url);
        CrawlerDocument crawlerDocument = null;
        try {
            crawlerDocument = CrawlerDocument.load(Data.gridIndex, urlid);
        } catch (IOException e) {
            // could not load the crawler document which is strange. It should be there
        }

        long t = System.currentTimeMillis();
        try {
            if (url.startsWith("http")) loadHTTP(warcWriter, url, threadName);
            else  if (url.startsWith("ftp")) loadFTP(warcWriter, url);
            else  if (url.startsWith("smb")) loadSMB(warcWriter, url);
            
            // write success status
            if (crawlerDocument != null) {
                long load_time = System.currentTimeMillis() - t;
                crawlerDocument.setStatus(Status.loaded).setStatusDate(new Date()).setComment("load time: " + load_time + " milliseconds");
                crawlerDocument.store(Data.gridIndex, urlid);
                // check with http://localhost:9200/crawler/_search?q=status_s:loaded
            }
        } catch (IOException e) {
            // write fail status
            if (crawlerDocument != null) {
                long load_time = System.currentTimeMillis() - t;
                crawlerDocument.setStatus(Status.load_failed).setStatusDate(new Date()).setComment("load fail: '" + e.getMessage() + "' after " + load_time + " milliseconds");
                crawlerDocument.store(Data.gridIndex, urlid);
                // check with http://localhost:9200/crawler/_search?q=status_s:load_failed
            }
            throw e;
        }
    }

    private static void loadFTP(WarcWriter warcWriter, String url) throws IOException {
        
    }
    
    private static void loadSMB(WarcWriter warcWriter, String url) throws IOException {
        
    }
    
    private static String userAgentDefault = BrowserVersion.CHROME.getUserAgent();
    static {
        ApacheHttpClient.initClient(userAgentDefault);
    }
    
    private static void loadHTTP(final WarcWriter warcWriter, final String url, final String threadName) throws IOException {// check short memory status
        if (Memory.shortStatus()) {
            ApacheHttpClient.initClient(userAgentDefault);
        }
        Date loaddate = new Date();

        // first do a HEAD request to find the mime type
        ApacheHttpClient ac = new ApacheHttpClient(url, true);
        
        // here we know the content type
        byte[] content = null;
        MultiProtocolURL u = new MultiProtocolURL(url);
        if (ac.getMime().endsWith("/html") || ac.getMime().endsWith("/xhtml+xml") || u.getContentDomainFromExt() == ContentDomain.TEXT) try {
            // use htmlunit to load this
            HtmlUnitLoader htmlUnitLoader = new HtmlUnitLoader(url, threadName);
            String xml = htmlUnitLoader.getXml();
            content = xml.getBytes(StandardCharsets.UTF_8);
        } catch (Throwable e) {
            // do nothing here, input stream is not set
            String cause = e == null ? "null" : e.getMessage();
            if (cause != null && cause.indexOf("404") >= 0) {
                throw new IOException("" + url + " fail: " + cause);
            }
            Data.logger.debug("Loader - HtmlUnit failed (will retry): " + cause);
        }
        
        if (content == null) {
            // do another http request. This can either happen because mime type is not html
            // or it was html and HtmlUnit has failed - we retry the normal way here.

            ac = new ApacheHttpClient(url, false);
            content = ac.getContent();
        }
        
        JwatWarcWriter.writeRequest(warcWriter, url, null, loaddate, null, null, ac.getRequestHeader().getBytes(StandardCharsets.UTF_8));

        // add the request header before the content
        ByteArrayOutputStream r = new ByteArrayOutputStream();
        r.write(ac.getResponseHeader().toString().getBytes(StandardCharsets.UTF_8));
        r.write(content);
        content = r.toByteArray();
        
        Data.logger.info("ContentLoader writing WARC for " + url + " - " + content.length + " bytes");
        JwatWarcWriter.writeResponse(warcWriter, url, null, loaddate, null, null, content);
    }
    
    public static void main(String[] args) {
        Data.init(new File("data/mcp-8100"), new HashMap<String, String>(), false);
        List<String> urls = new ArrayList<>();
        urls.add("https://krefeld.polizei.nrw/");
        //urls.add("https://www.justiz.nrw/Gerichte_Behoerden/anschriften/berlin_bruessel/index.php");
        byte[] warc = load(urls, "Test".getBytes(), false, "test");
        System.out.println(new String(warc, StandardCharsets.UTF_8));
    }
    
}
