package net.yacy.grid.loader.retrieval;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.RequestLine;

import net.yacy.grid.http.ClientConnection;
import net.yacy.grid.http.ClientIdentification;

public class JavaHttpClient implements HttpClient {

    private static final String CRLF = new String(ClientConnection.CRLF, StandardCharsets.US_ASCII);
    private static String userAgentDefault = ClientIdentification.browserAgent.userAgent;

    private int status_code;
    private String mime;
    private Map<String, List<String>> header;
    private String requestHeader, responseHeader;
    private byte[] content;
    
    public static void initClient(String userAgent) {
        userAgentDefault = userAgent;
    }
    
    public JavaHttpClient(String url, boolean head) throws IOException {
        
        HttpURLConnection connection = ((HttpURLConnection) new URL(url).openConnection());
        if (head) connection.setRequestMethod("HEAD");
        connection.addRequestProperty("User-Agent", userAgentDefault);
        
        // compute the request header (we do this to have a documentation later of what we did)
        Map<String, List<String>> map = connection.getRequestProperties();
        StringBuffer sb = new StringBuffer();
        String special =  connection.getHeaderField(0);
        sb.append(connection.getRequestMethod() + " " + url).append(CRLF);
        for (Map.Entry<String, List<String>> entry: connection.getRequestProperties().entrySet()) {
            String key = entry.getKey();
            for (String value: entry.getValue()) {
                sb.append(key).append(": ").append(value).append(CRLF);
            }
        }
        sb.append(CRLF);
        this.requestHeader = sb.toString();
        
        
        InputStream input;
        if (connection.getResponseCode() == 200)  // this must be called before 'getErrorStream()' works
            input = connection.getInputStream();
        else input = connection.getErrorStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String msg;
        while ((msg =reader.readLine()) != null)
            System.out.println(msg);
    }


    @Override
    public int getStatusCode() {
        return status_code;
    }

    @Override
    public String getMime() {
        return mime;
    }

    @Override
    public Map<String, List<String>> getHeader() {
        return header;
    }

    @Override
    public String getRequestHeader() {
        return requestHeader;
    }

    @Override
    public String getResponseHeader() {
        return responseHeader;
    }

    @Override
    public byte[] getContent() {
        return this.content;
    }
    
    public static void main(String[] args) {
        try {
            JavaHttpClient client = new JavaHttpClient("https://krefeld.polizei.nrw/", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
