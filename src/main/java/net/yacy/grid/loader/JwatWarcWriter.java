/**
 *  JwatWarcWriter
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

package net.yacy.grid.loader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import org.apache.commons.codec.binary.Base32;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;

import net.yacy.grid.tools.DateParser;

/**
 * for a documentation, see https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/
 * @author admin
 *
 */
public class JwatWarcWriter {
    
    public static void writeWarcinfo(WarcWriter writer, Date date, String warcinfo_uuid, String filename, byte[] payload) throws IOException {
        WarcRecord record = WarcRecord.createRecord(writer);
        record.header.addHeader("WARC-Type", "warcinfo");
        if (warcinfo_uuid != null) record.header.addHeader("WARC-Record-ID", "<urn:uuid:" + warcinfo_uuid + ">");
        record.header.addHeader("WARC-Date", DateParser.iso8601Format.format(date));
        if (filename != null) record.header.addHeader("WARC-Filename", filename);
        record.header.addHeader("Content-Length", Long.toString(payload.length));
        record.header.addHeader("Content-Type", "application/warc-fields");
        writer.writeHeader(record);
        ByteArrayInputStream inBytes = new ByteArrayInputStream(payload);
        writer.streamPayload(inBytes);
        writer.closeRecord();
    }

    public static void writeRequest(WarcWriter writer, String url, String ip, Date date, String warcrecord_uuid, String warcinfo_uuid, byte[] payload) throws IOException {
        WarcRecord record = WarcRecord.createRecord(writer);
        record.header.addHeader("WARC-Type", "request");
        record.header.addHeader("WARC-Target-URI", url);
        record.header.addHeader("Content-Type", "application/http;msgtype=request");
        record.header.addHeader("WARC-Date", DateParser.iso8601Format.format(date));
        if (warcrecord_uuid != null) record.header.addHeader("WARC-Record-ID", "<urn:uuid:" + warcrecord_uuid + ">");
        if (ip != null) record.header.addHeader("WARC-IP-Address", ip);
        if (warcinfo_uuid != null) record.header.addHeader("WARC-Warcinfo-ID", "<urn:uuid:" + warcinfo_uuid + ">");
        //record.header.addHeader("WARC-Block-Digest", "sha1:" + sha1(payload));
        record.header.addHeader("Content-Length", Long.toString(payload.length));
        writer.writeHeader(record);
        ByteArrayInputStream inBytes = new ByteArrayInputStream(payload);
        writer.streamPayload(inBytes);
        writer.closeRecord();
    }
    
    public static void writeResponse(WarcWriter writer, String url, String ip, Date date, String warcrecord_uuid, String warcinfo_uuid, byte[] payload) throws IOException {
        WarcRecord record = WarcRecord.createRecord(writer);
        record.header.addHeader("WARC-Type", "response");
        if (warcrecord_uuid != null) record.header.addHeader("WARC-Record-ID", "<urn:uuid:" + warcrecord_uuid + ">");
        if (warcinfo_uuid != null) record.header.addHeader("WARC-Warcinfo-ID", "<urn:uuid:" + warcinfo_uuid + ">");
        record.header.addHeader("WARC-Target-URI", url);
        record.header.addHeader("WARC-Date", DateParser.iso8601Format.format(date));
        if (ip != null) record.header.addHeader("WARC-IP-Address", ip);
        //record.header.addHeader("WARC-Block-Digest", "sha1:" + sha1(payload));
        //record.header.addHeader("WARC-Payload-Digest", "sha1:" + sha1(payload));
        record.header.addHeader("Content-Type", "application/http;msgtype=response");
        record.header.addHeader("Content-Length", Long.toString(payload.length));
        writer.writeHeader(record);
        ByteArrayInputStream inBytes = new ByteArrayInputStream(payload);
        writer.streamPayload(inBytes);
        writer.closeRecord();
    }
    
    /**
     * compute a sha1 in base32 format
     * We choosed that format, because WGET does the same
     * @param b
     * @return a base32 string for the sha1 of the input
     */
    public static String sha1(byte[] b) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            sha1.reset();
            sha1.update(b);
            return new Base32().encodeAsString(b);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }
    }
}
