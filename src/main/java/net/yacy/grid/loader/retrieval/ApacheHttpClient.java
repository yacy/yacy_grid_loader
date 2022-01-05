/**
 *  ApacheHttpClient
 *  Copyright 24.2.2018 by Michael Peter Christen, @0rb1t3r
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;
import net.yacy.grid.mcp.Logger;

public class ApacheHttpClient implements HttpClient {

    private static final String CRLF = new String(ClientConnection.CRLF, StandardCharsets.US_ASCII);

    private static CloseableHttpClient httpClient = null;
    private static String userAgentDefault = ClientIdentification.browserAgent.userAgent;

    private static ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
        @Override
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 60000;
        }
    };

    public static void initClient(String userAgent) {
        userAgentDefault = userAgent;
        HttpClientBuilder hcb = HttpClients.custom()
                .useSystemProperties()
                .setConnectionManager(getConnctionManager())
                .setMaxConnPerRoute(5)
                .setMaxConnTotal(Math.max(100, Runtime.getRuntime().availableProcessors() * 2))
                .setUserAgent(userAgentDefault)
                .setDefaultRequestConfig(ClientConnection.defaultRequestConfig)
                //.setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                .setKeepAliveStrategy(keepAliveStrategy)
                .setConnectionTimeToLive(60, TimeUnit.SECONDS);
         httpClient = hcb.build();
    }

    private int status_code;
    private String mime;
    private Map<String, List<String>> header;
    private String requestHeader, responseHeader;
    private byte[] content;

    public ApacheHttpClient(String url, boolean head) throws IOException {
        if (httpClient == null) initClient(ClientIdentification.browserAgent.userAgent);
        this.status_code = -1;
        this.content = null;
        this.mime = "";
        this.header = new HashMap<String, List<String>>();

        HttpResponse httpResponse = null;
        HttpRequestBase request = head ? new HttpHead(url) : new HttpGet(url);
        request.setHeader("User-Agent", userAgentDefault);
        request.setHeader("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");

        // compute the request header (we do this to have a documentation later of what we did)
        StringBuffer sb = new StringBuffer();
        RequestLine status = request.getRequestLine();
        sb.append(status.toString()).append(CRLF);
        for (Header h: request.getAllHeaders()) {
            sb.append(h.getName()).append(": ").append(h.getValue()).append(CRLF);
        }
        sb.append(CRLF);
        this.requestHeader = sb.toString();

        // do the request
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
        } catch (Throwable e) {
            request.releaseConnection();
            throw new IOException("error " + request.getURI() + ": " + e.getMessage());
        } finally {
            if (httpResponse != null) {
                this.status_code = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();
                if (head || this.status_code != 200) {
                    EntityUtils.consumeQuietly(httpEntity);
                    if (!head && this.status_code != 200) {
                        request.releaseConnection();
                        throw new IOException("client connection to " + url + " fail (status code " + this.status_code + "): " + httpResponse.getStatusLine().getReasonPhrase());
                    }
                } else {
                    try {
                        InputStream inputStream = new BufferedInputStream(httpEntity.getContent());
                        ByteArrayOutputStream r = new ByteArrayOutputStream();
                        byte[] b = new byte[1024];
                        int c;
                        while ((c = inputStream.read(b)) > 0) r.write(b, 0, c);
                        this.content = r.toByteArray();
                    } catch (IOException e) {
                        throw e;
                    }
                    Logger.info(this.getClass(), "ContentLoader loaded " + url);
                }

                // read response header and set mime
                if (this.status_code == 200 || this.status_code == 403) {
                    for (Header h: httpResponse.getAllHeaders()) {
                        List<String> vals = this.header.get(h.getName());
                        if (vals == null) { vals = new ArrayList<String>(); this.header.put(h.getName(), vals); }
                        vals.add(h.getValue());
                        if (h.getName().equals("Content-Type")) this.mime = h.getValue();
                    }
                }

                // fix mime in case a font is assigned
                int p = this.mime.indexOf(';');
                if (p >= 0) {
                    String charset = p < this.mime.length() - 2 ? this.mime.substring(p + 2) : "";
                    this.mime = this.mime.substring(0, p);
                    if (charset.startsWith("; charset=")) charset = charset.substring(10);
                }

                // compute response header string
                sb.setLength(0);
                sb.append(status.getProtocolVersion()).append(' ').append(this.status_code).append(CRLF);
                for (Map.Entry<String, List<String>> headers: this.header.entrySet()) {
                    for (String v: headers.getValue()) {
                        sb.append(headers.getKey()).append(": ").append(v).append(CRLF);
                    }
                }
                sb.append(CRLF);
                this.responseHeader = sb.toString();
            }
            request.releaseConnection();
        }
    }

    @Override
    public int getStatusCode() {
        return this.status_code;
    }

    @Override
    public String getMime() {
        return this.mime;
    }

    @Override
    public Map<String, List<String>> getHeader() {
        return this.header;
    }

    @Override
    public String getRequestHeader() {
        return this.requestHeader;
    }

    @Override
    public String getResponseHeader() {
        return this.responseHeader;
    }

    @Override
    public byte[] getContent() {
        return this.content;
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
            Logger.warn(e);
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
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    public static void main(String[] args) {
        try {
            initClient(ClientIdentification.browserAgent.userAgent);
            ApacheHttpClient client = new ApacheHttpClient("https://krefeld.polizei.nrw/", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
