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
import net.yacy.grid.tools.Logger;

/**
 * for a documentation, see https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/
 * @author admin
 *
 */
public class JwatWarcWriter {

    public static void writeWarcinfo(final WarcWriter writer, final Date date, final String warcinfo_uuid, final String filename, final byte[] payload) throws IOException {
        try {
            final WarcRecord record = WarcRecord.createRecord(writer);
            record.header.addHeader("WARC-Type", "warcinfo");
            if (warcinfo_uuid != null) record.header.addHeader("WARC-Record-ID", "<urn:uuid:" + warcinfo_uuid + ">");
            record.header.addHeader("WARC-Date", DateParser.iso8601Format.format(date));
            if (filename != null) record.header.addHeader("WARC-Filename", filename);
            record.header.addHeader("Content-Length", Long.toString(payload.length));
            record.header.addHeader("Content-Type", "application/warc-fields");
            writer.writeHeader(record);
            final ByteArrayInputStream inBytes = new ByteArrayInputStream(payload);
            writer.streamPayload(inBytes);
            writer.closeRecord(); // java.lang.NoSuchMethodError: java.nio.ByteBuffer.flip()Ljava/nio/ByteBuffer;
        } catch (final NoSuchMethodError e) {
            Logger.warn(e);
            throw new IOException(e.getMessage());
        // the writer may fail because of a java 8 class error
        /*
    java.lang.NoSuchMethodError: java.nio.ByteBuffer.flip()Ljava/nio/ByteBuffer;
    at org.jwat.gzip.GzipWriter$GzipEntryOutputStream.close(GzipWriter.java:513)
    at org.jwat.gzip.GzipEntry.close(GzipEntry.java:142)
    at org.jwat.warc.WarcWriterCompressed.closeRecord(WarcWriterCompressed.java:100)
         */
        }
    }

    public static void writeRequest(final WarcWriter writer, final String url, final String ip, final Date date, final String warcrecord_uuid, final String warcinfo_uuid, final byte[] payload) throws IOException {
        final WarcRecord record = WarcRecord.createRecord(writer);
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
        final ByteArrayInputStream inBytes = new ByteArrayInputStream(payload);
        writer.streamPayload(inBytes);
        writer.closeRecord();
    }

    public static void writeResponse(final WarcWriter writer, final String url, final String ip, final Date date, final String warcrecord_uuid, final String warcinfo_uuid, final byte[] payload) throws IOException {
        final WarcRecord record = WarcRecord.createRecord(writer);
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
        final ByteArrayInputStream inBytes = new ByteArrayInputStream(payload);
        writer.streamPayload(inBytes);
        writer.closeRecord();
    }

    /**
     * compute a sha1 in base32 format
     * We choosed that format, because WGET does the same
     * @param b
     * @return a base32 string for the sha1 of the input
     */
    public static String sha1(final byte[] b) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            sha1.reset();
            sha1.update(b);
            return new Base32().encodeAsString(b);
        } catch (final NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }
    }
}
