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

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiAction.RenderType;
import net.yacy.grid.io.index.CrawlerDocument;
import net.yacy.grid.io.index.CrawlerDocument.Status;
import net.yacy.grid.loader.JwatWarcWriter;
import net.yacy.grid.mcp.BrokerListener.ActionResult;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.Classification.ContentDomain;
import net.yacy.grid.tools.Digest;
import net.yacy.grid.tools.Logger;
import net.yacy.grid.tools.MultiProtocolURL;

public class ContentLoader {

    private byte[] content;
    private ActionResult result;

    public ContentLoader(
            final SusiAction action, final JSONArray data, final boolean compressed, final String threadnameprefix,
            final String id, final int depth, final int crawlingDepth, final boolean loaderHeadless, final int priority) {
        this.content = new byte[0];
        this.result = ActionResult.FAIL_IRREVERSIBLE;

        // this must have a loader action
        if (action.getRenderType() != RenderType.loader) {
            return;
        }

        // extract urls
        final JSONArray urls = action.getArrayAttr("urls");
        final List<String> urlss = new ArrayList<>();
        urls.forEach(u -> urlss.add(((String) u)));
        final byte[] warcPayload = data.toString(2).getBytes(StandardCharsets.UTF_8);

        // start loading
        Thread.currentThread().setName(threadnameprefix + " loading " + urlss.toString());

        // construct a WARC
        OutputStream out;
        File tmp = null;
        try {
            tmp = createTempFile("yacygridloader", ".warc");
            //Data.logger.info("creating temporary file: " + tmp.getAbsolutePath());
            out = new BufferedOutputStream(new FileOutputStream(tmp));
        } catch (final IOException e) {
            tmp = null;
            out = new ByteArrayOutputStream();
        }
        try {
            final WarcWriter ww = ContentLoader.initWriter(out, warcPayload, compressed);
            final Map<String, ActionResult> errors = ContentLoader.load(ww, urlss, threadnameprefix, id, depth, crawlingDepth, loaderHeadless, priority);
            this.result = ActionResult.SUCCESS;
            errors.forEach((u, c) -> {
                Logger.debug(this.getClass(), "Loader - cannot load: " + u + " - " + c);
                if (c == ActionResult.FAIL_RETRY && this.result == ActionResult.SUCCESS) this.result = ActionResult.FAIL_RETRY;
                if (c == ActionResult.FAIL_IRREVERSIBLE) this.result = ActionResult.FAIL_IRREVERSIBLE;
            });
        } catch (final IOException e) {
            Logger.warn(this.getClass(), "ContentLoader WARC writer init problem", e);
        } finally {
            if (out != null) try {out.close();} catch (final IOException e) {}
        }
        if (out instanceof ByteArrayOutputStream) {
            this.content = ((ByteArrayOutputStream) out).toByteArray();
            this.result = ActionResult.SUCCESS;
            return;
        } else {
            try {
                // open the file again to create a byte[]
                this.content = Files.readAllBytes(tmp.toPath());
                this.result = ActionResult.SUCCESS;
                tmp.delete();
                if (tmp.exists()) tmp.deleteOnExit();
                return;
            } catch (final IOException e) {
                // this should not happen since we had been able to open the file
                Logger.warn(this.getClass(), e);
                return; // fail irreversible
            }
        }
    }


    public byte[] getContent() {
        return this.content;
    }

    public ActionResult getResult() {
        return this.result;
    }

    private final static SimpleDateFormat millisFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.US);
    private final static AtomicLong createTempFileCounter = new AtomicLong(0);
    public static File createTempFile(final String prefix, final String suffix) throws IOException {
        final String tmpprefix = prefix + "-" + millisFormat.format(new Date()) + Long.toString(createTempFileCounter.getAndIncrement());
        final File tmp = File.createTempFile(tmpprefix, suffix);
        return tmp;
    }

    private static WarcWriter initWriter(final OutputStream out, final byte[] payload, final boolean compressed) throws IOException {
        final WarcWriter ww = WarcWriterFactory.getWriter(out, compressed);
        JwatWarcWriter.writeWarcinfo(ww, new Date(), null, null, payload);
        return ww;
    }

    private static Map<String, ActionResult> load(
            final WarcWriter warcWriter, final List<String> urls, final String threadName,
            final String id, final int depth, final int crawlingDepth, final boolean loaderHeadless, final int priority) throws IOException {

        // this is here for historical reasons, we actually should have all urls normalized
        final List<String> fixedURLs = new ArrayList<>();
        urls.forEach(url -> {
            if (url.indexOf("//") < 0) url = "http://" + url;
            fixedURLs.add(url);
        });

        // prepare map with ids and load crawlerDocuments
        final Map<String, String> urlmap = new HashMap<>();
        fixedURLs.forEach(url -> urlmap.put(url, Digest.encodeMD5Hex(url)));
        final Map<String, CrawlerDocument> crawlerDocuments = CrawlerDocument.loadBulk(Service.instance.config, Service.instance.config.gridIndex, urlmap.values());

        // load content
        final Map<String, ActionResult> errors = new LinkedHashMap<>();
        fixedURLs.forEach(url -> {

            // do loader throttling here
            long throttling = 250;
            try {
                throttling = Service.instance.config.gridControl.checkThrottling(id, url, depth, crawlingDepth, loaderHeadless, priority);
            } catch (final IOException e1) {}
            Thread.currentThread().setName(threadName + " loading " + url.toString() + ", throttling = " + throttling);
            try {Thread.sleep(throttling);} catch (final InterruptedException e) {}

            // start loading
            try {
                // load entry from crawler index
                final String urlid = urlmap.get(url);
                final CrawlerDocument crawlerDocument = crawlerDocuments.get(urlid);
                //assert crawlerDocument != null;

                // load content from the network
                final long t = System.currentTimeMillis();
                try {
                    boolean success = false;
                    if (url.startsWith("http")) success = loadHTTP(warcWriter, url, threadName, loaderHeadless);
                    else  if (url.startsWith("ftp")) loadFTP(warcWriter, url);
                    else  if (url.startsWith("smb")) loadSMB(warcWriter, url);

                    // write success status
                    if (success && crawlerDocument != null) {
                        final long load_time = System.currentTimeMillis() - t;
                        crawlerDocument.setStatus(Status.loaded).setStatusDate(new Date()).setComment("load time: " + load_time + " milliseconds");
                        // crawlerDocument.store(Data.gridIndex); we bulk-store this later
                        // check with http://localhost:9200/crawler/_search?q=status_s:loaded
                    }
                } catch (final IOException e) {
                    // write fail status
                    if (crawlerDocument != null) {
                        final long load_time = System.currentTimeMillis() - t;
                        crawlerDocument.setStatus(Status.load_failed).setStatusDate(new Date()).setComment("load fail: '" + e.getMessage() + "' after " + load_time + " milliseconds");
                        // crawlerDocument.store(Data.gridIndex); we bulk-store this later
                        // check with http://localhost:9200/crawler/_search?q=status_s:load_failed
                    }
                }
            } catch (final Throwable e) {
                Logger.warn("ContentLoader cannot load " + url + " - " + e.getMessage());
                errors.put(url, ActionResult.FAIL_IRREVERSIBLE);
            }
        });

        // bulk-store the crawler documents
        try {
            CrawlerDocument.storeBulk(Service.instance.config, Service.instance.config.gridIndex, crawlerDocuments);
        } catch (final Throwable e) {
            Logger.error(e);
        }
        return errors;
    }

    private static void loadFTP(final WarcWriter warcWriter, final String url) throws IOException {

    }

    private static void loadSMB(final WarcWriter warcWriter, final String url) throws IOException {

    }

    private static boolean loadHTTP(final WarcWriter warcWriter, final String url, final String threadName, final boolean useHeadlessLoader) throws IOException {// check short memory status
        final Date loaddate = new Date();

        // first do a HEAD request to find the mime type
        ApacheHttpClient ac = new ApacheHttpClient(url, true);

        // here we know the content type
        byte[] content = null;

        String requestHeaders = ac.getRequestHeader().toString();
        String responseHeaders = ac.getResponseHeader().toString();

        final MultiProtocolURL u = new MultiProtocolURL(url);
        if (useHeadlessLoader && (ac.getMime().endsWith("/html") || ac.getMime().endsWith("/xhtml+xml") || u.getContentDomainFromExt() == ContentDomain.TEXT)) try {
            // use htmlunit to load this
            final HtmlUnitLoader htmlUnitLoader = new HtmlUnitLoader(url, threadName);
            final String xml = htmlUnitLoader.getXml();

            requestHeaders = htmlUnitLoader.getRequestHeaders();
            responseHeaders = htmlUnitLoader.getResponseHeaders();

            content = xml.getBytes(StandardCharsets.UTF_8);
        } catch (final Throwable e) {
            // do nothing here, input stream is not set
            final String cause = e == null ? "null" : e.getMessage();
            if (cause != null && cause.indexOf("404") >= 0) {
                throw new IOException("" + url + " fail: " + cause);
            }
            Logger.debug("Loader - HtmlUnit failed (will retry): " + cause);
        }

        if (content == null) {
            // do another http request. This can either happen because mime type is not html
            // or it was html and HtmlUnit has failed - we retry the normal way here.

            ac = new ApacheHttpClient(url, false);
            final int status = ac.getStatusCode();
            if (status != 200) return false;

            requestHeaders = ac.getRequestHeader().toString();
            responseHeaders = ac.getResponseHeader().toString();

            content = ac.getContent();
        }

        if (content == null || content.length == 0) return false;

        JwatWarcWriter.writeRequest(warcWriter, url, null, loaddate, null, null, requestHeaders.getBytes(StandardCharsets.UTF_8));

        // add the request header before the content
        final ByteArrayOutputStream r = new ByteArrayOutputStream();
        r.write(responseHeaders.getBytes(StandardCharsets.UTF_8));
        r.write(content);
        content = r.toByteArray();

        Logger.info("ContentLoader writing WARC for " + url + " - " + content.length + " bytes");
        JwatWarcWriter.writeResponse(warcWriter, url, null, loaddate, null, null, content);

        return true;
    }

    /*
    public static void main(String[] args) {
        Data.init(new File("data/mcp-8100"), new HashMap<String, String>(), false);
        List<String> urls = new ArrayList<>();
        urls.add("https://krefeld.polizei.nrw/");
        //urls.add("https://www.justiz.nrw/Gerichte_Behoerden/anschriften/berlin_bruessel/index.php");
        byte[] warc = load(urls, "Test".getBytes(), false, "test", true);
        System.out.println(new String(warc, StandardCharsets.UTF_8));
    }
    */

}
