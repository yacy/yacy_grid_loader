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

import org.json.JSONObject;

import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;


public class LoaderService extends ObjectAPIHandler implements APIHandler {

    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "loader";
    
    @Override
    public String getAPIPath() {
        return "/yacy/grid/loader/" + NAME + ".json";
    }
    
    /*
     * engines to be considered:
     * 
     * https://www.teamdev.com/jxbrowser
     * no maven repository!
     * 
     * http://phantomjs.org/
     * has it's own binary JavaScript framework which is not easy to integrate
     * 
     * http://jaunt-api.com/
     * That has a just crazy monthly-expiring license and no maven support
     * 
     * http://htmlunit.sourceforge.net/
     * good!
     */
    
    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {

        return new ServiceResponse(new JSONObject());
    }

}
