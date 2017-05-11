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

import javax.servlet.http.HttpServletResponse;

import ai.susi.mind.SusiThought;
import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;
import net.yacy.grid.loader.HttpLoader;

/**
 * 
 * Test URL:
 * http://localhost:8200/yacy/grid/loader/warcloader.warc.gz?url=http://yacy.net
 * 
 * Test command:
 * curl -o yacy.net.warc.gz "http://localhost:8200/yacy/grid/loader/warcloader.warc.gz?collection=test&url=http://yacy.net"
 * parse this warc with:
 * curl -X POST -F "sourcebytes=@yacy.net.warc.gz;type=application/octet-stream" http://127.0.0.1:8500/yacy/grid/parser/parser.json
 */
public class LoaderService extends ObjectAPIHandler implements APIHandler {

    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "warcloader";
    
    @Override
    public String getAPIPath() {
        return "/yacy/grid/loader/" + NAME + ".warc.gz";
    }

    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {
        // construct the same process as if it was submitted on a queue
        SusiThought process = ProcessService.queryToProcess(call);
        
        // construct a WARC
        byte[] b = HttpLoader.eval(process, true);
        
        // store the WARC as asset if wanted
        return new ServiceResponse(b);
    }

}

