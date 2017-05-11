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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;
import net.yacy.grid.mcp.Data;

/**
 * 
 * Test URL:
 * http://localhost:8200/yacy/grid/loader/warcloader.warc.gz?url=http://yacy.net
 * 
 * Test command:
 * curl -o yacy.net.warc.gz http://localhost:8200/yacy/grid/loader/warcloader.warc.gz?url=http://yacy.net
 * parse this warc with:
 * curl -X POST -F "sourcebytes=@yacy.net.warc.gz;type=application/octet-stream" http://127.0.0.1:8500/yacy/grid/parser/parser.json
 */
public class LoaderService extends ObjectAPIHandler implements APIHandler {

    private static final String CRLF = new String(ClientConnection.CRLF, StandardCharsets.US_ASCII);
    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "warcloader";
    
    @Override
    public String getAPIPath() {
        return "/yacy/grid/loader/" + NAME + ".warc.gz";
    }

    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {
        String url = call.get("url", "");
        Date loaddate = new Date();
        if (url.length() > 0) try {
            Map<String, List<String>> header = new HashMap<String, List<String>>();
            int statuscode;
            InputStream inputStream;
            CloseableHttpClient httpClient = HttpClients.custom()
                    .useSystemProperties()
                    .setConnectionManager(ClientConnection.getConnctionManager(false))
                    .setDefaultRequestConfig(ClientConnection.defaultRequestConfig)
                    .build();
            
            // first do a HEAD request to find the mime type
            HttpRequestBase request = new HttpHead(url);
            request.setHeader("User-Agent", ClientIdentification.getAgent(ClientIdentification.yacyInternetCrawlerAgentName).userAgent);
            

            HttpResponse httpResponse = null;
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
            String mime = "";
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
            if (mime.endsWith("/html")) {
                // use htmlunit to load this
                HtmlUnitLoader htmlUnitLoader = new HtmlUnitLoader(url);
                String xml = htmlUnitLoader.getXml();
                inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
            } else {
                // do another http request

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
            
            // construct a WARC
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            WarcWriter ww = WarcWriterFactory.getWriter(baos, true);
            JwatWarcWriter.writeWarcinfo(ww, new Date(), null, null, "yacy_grid_loader".getBytes());
            
            // compute the request
            StringBuffer sb = new StringBuffer();
            RequestLine status = request.getRequestLine();
            sb.append(status.toString()).append(CRLF);
            for (Header h: request.getAllHeaders()) {
                sb.append(h.getName()).append(": ").append(h.getValue()).append(CRLF);
            }
            sb.append(CRLF);
            JwatWarcWriter.writeRequest(ww, url, null, loaddate, null, null, sb.toString().getBytes(StandardCharsets.UTF_8));

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
            JwatWarcWriter.writeResponse(ww, url, null, loaddate, null, null, r.toByteArray());
            
            return new ServiceResponse(baos.toByteArray());
        } catch (IOException e) {
            Data.logger.error(e.getMessage(), e);
        }
        
        return new ServiceResponse("");
    }

}

