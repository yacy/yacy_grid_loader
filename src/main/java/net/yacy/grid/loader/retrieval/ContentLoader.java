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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLHandshakeException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiAction.RenderType;
import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;
import net.yacy.grid.loader.JwatWarcWriter;
import net.yacy.grid.mcp.Data;

public class ContentLoader {
    
    private static final String CRLF = new String(ClientConnection.CRLF, StandardCharsets.US_ASCII);
    private static final AtomicInteger fc = new AtomicInteger(0);
    public final static SimpleDateFormat millisFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.US);
    
    public static byte[] eval(SusiAction action, JSONArray data, boolean compressed) {
        if (action.getRenderType() == RenderType.loader) {
            // construct a WARC
            String tmpfilename = "yacygridloader-" + millisFormat.format(new Date());
            tmpfilename += "-" + Integer.toString(fc.incrementAndGet());
            OutputStream out;
            File tmp = null;
            try {
                tmp = File.createTempFile(tmpfilename, ".warc");
                //Data.logger.info("creating temporary file: " + tmp.getAbsolutePath());
                out = new BufferedOutputStream(new FileOutputStream(tmp));
            } catch (IOException e) {
                tmp = null;
                out = new ByteArrayOutputStream();
            }
            WarcWriter ww = ContentLoader.initWriter(out, data, compressed);
            JSONArray urls = action.getArrayAttr("urls");
            Map<String, String> errors = ContentLoader.load(ww, urls);
            errors.forEach((u, c) -> Data.logger.debug("Loader - cannot load: " + u + " - " + c));
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
                    e.printStackTrace();
                }
            }
        }
        return new byte[0];
    }
    
    public static WarcWriter initWriter(OutputStream out, JSONArray data, boolean compressed) {
        WarcWriter ww = WarcWriterFactory.getWriter(out, compressed);
        
        try {
            JwatWarcWriter.writeWarcinfo(ww, new Date(), null, null, data.toString(2).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ww;
    }

    public static List<String> load(WarcWriter warcWriter, List<String> urls) {
        List<String> errors = new ArrayList<>();
        urls.forEach(url -> {
            try {
                load(warcWriter, url);
            } catch (IOException e) {
                e.printStackTrace();
                errors.add(url);
            }
        });
        return errors;
    }
    
    public static Map<String, String> load(WarcWriter warcWriter, JSONArray urls) {
        Map<String, String> errors = new LinkedHashMap<>();
        urls.forEach(url -> {
            try {
                load(warcWriter, (String) url);
            } catch (Throwable e) {
                e.printStackTrace();
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
    
    private static void loadHTTP(WarcWriter warcWriter, String url) throws IOException {
        Date loaddate = new Date();
        
        // first do a HEAD request to find the mime type
        CloseableHttpClient httpClient = HttpClients.custom()
                .useSystemProperties()
                .setConnectionManager(ClientConnection.getConnctionManager(false))
                .setDefaultRequestConfig(ClientConnection.defaultRequestConfig)
                .build();
        
        HttpRequestBase request = new HttpHead(url);
        request.setHeader("User-Agent", ClientIdentification.getAgent(ClientIdentification.browserAgentName/*.yacyInternetCrawlerAgentName*/).userAgent);
        
        HttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(request);
        } catch (UnknownHostException e) {
            request.releaseConnection();
            throw new IOException("client connection failed: unknown host " + request.getURI().getHost());
        } catch (SocketTimeoutException e) {
            request.releaseConnection();
            throw new IOException("client connection timeout for request: " + request.getURI());
        } catch (SSLHandshakeException e) {
            request.releaseConnection();
            throw new IOException("client connection handshake error for domain " + request.getURI().getHost() + ": " + e.getMessage());
        } catch (HttpHostConnectException e) {
            request.releaseConnection();
            throw new IOException("client connection refused for request " + request.getURI() + ": " + e.getMessage());
        }

        int statuscode = httpResponse.getStatusLine().getStatusCode();
        String mime = "";
        Map<String, List<String>> header = new HashMap<String, List<String>>();
        if (statuscode == 200) {
            for (Header h: httpResponse.getAllHeaders()) {
                List<String> vals = header.get(h.getName());
                if (vals == null) { vals = new ArrayList<String>(); header.put(h.getName(), vals); }
                vals.add(h.getValue());
                if (h.getName().equals("Content-Type")) mime = h.getValue();
            }
        } else {
            request.releaseConnection();
            throw new IOException("client connection to " + request.getURI() + " fail: " + httpResponse.getStatusLine().getReasonPhrase());
        }
        
        // here we know the content type
        InputStream inputStream = null;
        if (mime.endsWith("/html") || mime.endsWith("/xhtml+xml")) try {
            // use htmlunit to load this
            HtmlUnitLoader htmlUnitLoader = new HtmlUnitLoader(url);
            String xml = htmlUnitLoader.getXml();
            inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            // do nothing here, input stream is not set
            String cause = e.getMessage();
            if (cause.indexOf("404") >= 0) throw new IOException("" + request.getURI() + " fail: " + cause);
            Data.logger.debug("Loader - HtmlUnit failed (will retry): " + cause);
        }
        
        if (inputStream == null) {
            // do another http request. This can either happen because mime type is not html
            // or it was html and HtmlUnit has failed - we retry the normal way here.

            // do a GET request
            request = new HttpGet(url);
            request.setHeader("User-Agent", ClientIdentification.getAgent(ClientIdentification.yacyInternetCrawlerAgentName).userAgent);
            
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
            }
            statuscode = httpResponse.getStatusLine().getStatusCode();
            
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                if (statuscode == 200) {
                    try {
                        inputStream = new BufferedInputStream(httpEntity.getContent());
                    } catch (IOException e) {
                        request.releaseConnection();
                        throw e;
                    }
                    for (Header h: httpResponse.getAllHeaders()) {
                        List<String> vals = header.get(h.getName());
                        if (vals == null) { vals = new ArrayList<String>(); header.put(h.getName(), vals); }
                        vals.add(h.getValue());
                    }
                } else {
                    request.releaseConnection();
                    throw new IOException("client connection to " + request.getURI() + " fail: " + httpResponse.getStatusLine().getReasonPhrase());
                }
            } else {
                request.releaseConnection();
                throw new IOException("client connection to " + request.getURI() + " fail: no connection");
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
        JwatWarcWriter.writeResponse(warcWriter, url, null, loaddate, null, null, r.toByteArray());
    }
    
}
