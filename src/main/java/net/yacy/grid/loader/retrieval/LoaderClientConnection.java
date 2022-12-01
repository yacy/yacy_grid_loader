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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLHandshakeException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;
import net.yacy.grid.tools.Logger;

public class LoaderClientConnection implements HttpClient {

    private static final String CRLF = new String(ClientConnection.CRLF, StandardCharsets.US_ASCII);

    public  static String userAgent = ClientIdentification.browserAgent.userAgent;
    private static CloseableHttpClient httpClient = ClientConnection.getClosableHttpClient(userAgent);
    private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(30);

    static {
        RequestConfig config = RequestConfig.custom()
          .setConnectTimeout(10000)
          .setConnectionRequestTimeout(10000)
          .setSocketTimeout(10000).build();
        httpClient = 
          HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    private int status_code;
    private String mime;
    private final Map<String, List<String>> header;
    private final String requestHeader;

    private String responseHeader;
    private byte[] content;

    public LoaderClientConnection(final String url, final boolean head) throws IOException {
        this.status_code = -1;
        this.content = null;
        this.mime = "";
        this.header = new HashMap<String, List<String>>();

        final HttpRequestBase request = head ? new HttpHead(url) : new HttpGet(url);
        request.setHeader("User-Agent", userAgent);
        request.setHeader("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");

        // compute the request header (we do this to have a documentation later of what we did)
        final StringBuffer sb = new StringBuffer();
        final RequestLine status = request.getRequestLine();
        sb.append(status.toString()).append(CRLF);
        for (final Header h: request.getAllHeaders()) {
            sb.append(h.getName()).append(": ").append(h.getValue()).append(CRLF);
        }
        sb.append(CRLF);
        this.requestHeader = sb.toString();

        // do the request
        HttpResponse httpResponse = null;
        try {
            executorService.schedule(request::abort, (long)10, TimeUnit.SECONDS);
            httpResponse = httpClient.execute(request);
        } catch (final UnknownHostException e) {
            request.releaseConnection();
            throw new IOException("client connection failed: unknown host " + request.getURI().getHost());
        } catch (final SocketTimeoutException e) {
            request.releaseConnection();
            throw new IOException("client connection timeout for request: " + request.getURI());
        } catch (final SSLHandshakeException e) {
            request.releaseConnection();
            throw new IOException("client connection handshake error for domain " + request.getURI().getHost() + ": " + e.getMessage());
        } catch (final HttpHostConnectException e) {
            request.releaseConnection();
            throw new IOException("client connection refused for request " + request.getURI() + ": " + e.getMessage());
        } catch (final Throwable e) {
            request.releaseConnection();
            throw new IOException("error " + request.getURI() + ": " + e.getMessage());
        } finally {
            if (httpResponse != null) {
                this.status_code = httpResponse.getStatusLine().getStatusCode();
                final HttpEntity httpEntity = httpResponse.getEntity();
                if (head || this.status_code != 200) {
                    EntityUtils.consumeQuietly(httpEntity);
                    if (!head && this.status_code != 200) {
                        request.releaseConnection();
                        throw new IOException("client connection to " + url + " fail (status code " + this.status_code + "): " + httpResponse.getStatusLine().getReasonPhrase());
                    }
                } else {
                    try {
                        final InputStream inputStream = new BufferedInputStream(httpEntity.getContent());
                        final ByteArrayOutputStream r = new ByteArrayOutputStream();
                        final byte[] b = new byte[1024];
                        int c;
                        while ((c = inputStream.read(b)) > 0) r.write(b, 0, c);
                        this.content = r.toByteArray();
                    } catch (final IOException e) {
                        throw e;
                    }
                    Logger.info(this.getClass(), "ContentLoader loaded " + url);
                }

                // read response header and set mime
                if (this.status_code == 200 || this.status_code == 403) {
                    for (final Header h: httpResponse.getAllHeaders()) {
                        List<String> vals = this.header.get(h.getName());
                        if (vals == null) { vals = new ArrayList<String>(); this.header.put(h.getName(), vals); }
                        vals.add(h.getValue());
                        if (h.getName().equals("Content-Type")) this.mime = h.getValue();
                    }
                }

                // fix mime in case a font is assigned
                final int p = this.mime.indexOf(';');
                if (p >= 0) {
                    String charset = p < this.mime.length() - 2 ? this.mime.substring(p + 2) : "";
                    this.mime = this.mime.substring(0, p);
                    if (charset.startsWith("; charset=")) charset = charset.substring(10);
                }

                // compute response header string
                sb.setLength(0);
                sb.append(status.getProtocolVersion()).append(' ').append(this.status_code).append(CRLF);
                for (final Map.Entry<String, List<String>> headers: this.header.entrySet()) {
                    for (final String v: headers.getValue()) {
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

    public static void main(final String[] args) {
        try {
            //final LoaderClientConnection client = new LoaderClientConnection("https://yacy.net", false);
            final LoaderClientConnection client = new LoaderClientConnection("https://morrismuseum.org/", false);

            final int status = client.getStatusCode();
            System.out.println("status: " + status);
            //String requestHeaders = client.getRequestHeader().toString();
            //String responseHeaders = client.getResponseHeader().toString();
            System.out.println(new String(client.getContent()));

        } catch (final IOException e) {
            e.printStackTrace();
        }
    }
}
