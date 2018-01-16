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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiAction.RenderType;
import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;
import net.yacy.grid.loader.JwatWarcWriter;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.tools.Classification.ContentDomain;
import net.yacy.grid.tools.Memory;
import net.yacy.grid.tools.MultiProtocolURL;

public class ContentLoader {
    
    private static final String CRLF = new String(ClientConnection.CRLF, StandardCharsets.US_ASCII);
    
    public static byte[] eval(SusiAction action, JSONArray data, boolean compressed) {
        // this must have a loader action
        if (action.getRenderType() != RenderType.loader) return new byte[0];
        
        // extract urls
        JSONArray urls = action.getArrayAttr("urls");
        List<String> urlss = new ArrayList<>();
        urls.forEach(u -> urlss.add(((String) u)));
        byte[] payload = data.toString(2).getBytes(StandardCharsets.UTF_8);
        return load(urlss, payload, compressed);
    }

    public static byte[] load(List<String> urls, byte[] header, boolean compressed) {
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
            Map<String, String> errors = ContentLoader.load(ww, urls);
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
    
    public static Map<String, String> load(WarcWriter warcWriter, List<String> urls) {
        Map<String, String> errors = new LinkedHashMap<>();
        urls.forEach(url -> {
            try {
                load(warcWriter, url);
            } catch (Throwable e) {
                Data.logger.warn("ContentLoader cannot load " + url + " - " + e.getMessage(), e);
                errors.put((String) url, e.getMessage());
            }
        });
        return errors;
    }

    public static void load(WarcWriter warcWriter, String url) throws IOException {
        if (url.indexOf("//") < 0) url = "http://" + url;
        if (url.startsWith("http")) loadHTTP(warcWriter, url);
        else  if (url.startsWith("ftp")) loadFTP(warcWriter, url);
        else  if (url.startsWith("smb")) loadSMB(warcWriter, url);
    }

    private static void loadFTP(WarcWriter warcWriter, String url) throws IOException {
        
    }
    
    private static void loadSMB(WarcWriter warcWriter, String url) throws IOException {
        
    }
    
    private static CloseableHttpClient httpClient;
    private static void init() {
        httpClient = HttpClients.custom()
                .useSystemProperties()
                .setConnectionManager(getConnctionManager())
                .setMaxConnPerRoute(2000)
                .setMaxConnTotal(3000)
                .setDefaultRequestConfig(ClientConnection.defaultRequestConfig)
                .build();
    }
    static {
        init();
    }
    
    private static void loadHTTP(WarcWriter warcWriter, String url) throws IOException {// check short memory status
        if (Memory.shortStatus()) {
            init();
        }
        Date loaddate = new Date();
        
        // first do a HEAD request to find the mime type
        HttpResponse httpResponse = null;
        HttpRequestBase request = new HttpHead(url);
        request.setHeader("User-Agent", ClientIdentification.getAgent(ClientIdentification.googleAgentName/*.yacyInternetCrawlerAgentName*/).userAgent);
        try {
            httpResponse = httpClient.execute(request);
        } catch (UnknownHostException e) {
            throw new IOException("client connection failed: unknown host " + request.getURI().getHost());
        } catch (SocketTimeoutException e) {
            //throw new IOException("client connection timeout for request: " + request.getURI());
        } catch (SSLHandshakeException e) {
            //throw new IOException("client connection handshake error for domain " + request.getURI().getHost() + ": " + e.getMessage());
        } catch (HttpHostConnectException e) {
            //throw new IOException("client connection refused for request " + request.getURI() + ": " + e.getMessage());
        } catch (Throwable e) {
        } finally {
            if (httpResponse != null) EntityUtils.consumeQuietly(httpResponse.getEntity());
            request.releaseConnection();
        }
        
        int statuscode = httpResponse == null ? -1 : httpResponse.getStatusLine().getStatusCode();
        String mime = "";
        Map<String, List<String>> header = new HashMap<String, List<String>>();
        if (statuscode == 200) {
            for (Header h: httpResponse.getAllHeaders()) {
                List<String> vals = header.get(h.getName());
                if (vals == null) { vals = new ArrayList<String>(); header.put(h.getName(), vals); }
                vals.add(h.getValue());
                if (h.getName().equals("Content-Type")) mime = h.getValue();
            }
        }
        
        // here we know the content type
        InputStream inputStream = null;
        MultiProtocolURL u = new MultiProtocolURL(url);
        if (mime.endsWith("/html") || mime.endsWith("/xhtml+xml") || u.getContentDomainFromExt() == ContentDomain.TEXT) try {
            // use htmlunit to load this
            HtmlUnitLoader htmlUnitLoader = new HtmlUnitLoader(url);
            String xml = htmlUnitLoader.getXml();
            inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        } catch (Throwable e) {
            // do nothing here, input stream is not set
            String cause = e == null ? "null" : e.getMessage();
            if (cause != null && cause.indexOf("404") >= 0) throw new IOException("" + url + " fail: " + cause);
            Data.logger.debug("Loader - HtmlUnit failed (will retry): " + cause);
        }
        
        if (inputStream == null) {
            // do another http request. This can either happen because mime type is not html
            // or it was html and HtmlUnit has failed - we retry the normal way here.

            // do a GET request
            request = new HttpGet(url);
            request.setHeader("User-Agent", ClientIdentification.getAgent(ClientIdentification.googleAgentName).userAgent);
            
            try {
                httpResponse = httpClient.execute(request);
            } catch (UnknownHostException e) {
                request.releaseConnection();
                throw new IOException("client connection failed: unknown host " + request.getURI().getHost());
            } catch (SocketTimeoutException e){
                request.releaseConnection();
                throw new IOException("client connection timeout for request: " + request.getURI());
            } catch (SSLHandshakeException e){
                request.releaseConnection();
                throw new IOException("client connection handshake error for domain " + request.getURI().getHost() + ": " + e.getMessage());
            } catch (HttpHostConnectException e){
                request.releaseConnection();
                throw new IOException("client connection refused for domain " + request.getURI().getHost() + ": " + e.getMessage());
            }
            statuscode = httpResponse.getStatusLine().getStatusCode();
            
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                if (statuscode == 200) {
                    try {
                        inputStream = new BufferedInputStream(httpEntity.getContent());
                    } catch (IOException e) {
                        throw e;
                    }
                    for (Header h: httpResponse.getAllHeaders()) {
                        List<String> vals = header.get(h.getName());
                        if (vals == null) { vals = new ArrayList<String>(); header.put(h.getName(), vals); }
                        vals.add(h.getValue());
                    }
                    Data.logger.info("ContentLoader loaded " + url);
                } else {
                    EntityUtils.consumeQuietly(httpEntity);
                    request.releaseConnection();
                    throw new IOException("client connection to " + url + " fail: " + httpResponse.getStatusLine().getReasonPhrase());
                }
            } else {
                request.releaseConnection();
                throw new IOException("client connection to " + url + " fail: no connection");
            }
        }
        // compute the request
        StringBuffer sb = new StringBuffer();
        RequestLine status = request.getRequestLine();
        sb.append(status.toString()).append(CRLF);
        for (Header h: request.getAllHeaders()) {
            sb.append(h.getName()).append(": ").append(h.getValue()).append(CRLF);
        }
        sb.append(CRLF);
        JwatWarcWriter.writeRequest(warcWriter, url, null, loaddate, null, null, sb.toString().getBytes(StandardCharsets.UTF_8));

        // compute response
        sb.setLength(0);
        sb.append(status.getProtocolVersion()).append(' ').append(statuscode).append(CRLF);
        for (Map.Entry<String, List<String>> headers: header.entrySet()) {
            for (String v: headers.getValue()) {
                sb.append(headers.getKey()).append(": ").append(v).append(CRLF);
            }
        }
        sb.append(CRLF);
        ByteArrayOutputStream r = new ByteArrayOutputStream();
        r.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        byte[] b = new byte[1024];
        int c;
        while ((c = inputStream.read(b)) > 0) r.write(b, 0, c);

        request.releaseConnection();
        byte[] content = r.toByteArray();
        Data.logger.info("ContentLoader writing WARC for " + url + " - " + content.length + " bytes");
        JwatWarcWriter.writeResponse(warcWriter, url, null, loaddate, null, null, content);
    }

    /**
     * get a connection manager
     * @param trustAllCerts allow opportunistic encryption if needed
     * @return
     */
    private static HttpClientConnectionManager getConnctionManager() {

        Registry<ConnectionSocketFactory> socketFactoryRegistry = null;
        try {
            SSLConnectionSocketFactory trustSelfSignedSocketFactory = new SSLConnectionSocketFactory(
                        new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(),
                        new TrustAllHostNameVerifier());
            socketFactoryRegistry = RegistryBuilder
                    .<ConnectionSocketFactory> create()
                    .register("http", new PlainConnectionSocketFactory())
                    .register("https", trustSelfSignedSocketFactory)
                    .build();
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
            Data.logger.warn("", e);
        }

        PoolingHttpClientConnectionManager cm = (socketFactoryRegistry != null) ?
                new PoolingHttpClientConnectionManager(socketFactoryRegistry):
                new PoolingHttpClientConnectionManager();

        // twitter specific options
        cm.setMaxTotal(2000);
        cm.setDefaultMaxPerRoute(200);
        
        return cm;
    }
    private static class TrustAllHostNameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    public static void main(String[] args) {
        Data.init(new File("data/mcp-8100"), new HashMap<String, String>());
        List<String> urls = new ArrayList<>();
        urls.add("https://www.justiz.nrw/Gerichte_Behoerden/anschriften/berlin_bruessel/index.php");
        byte[] warc = load(urls, "Test".getBytes(), false);
        System.out.println(new String(warc, StandardCharsets.UTF_8));
    }
    
}
