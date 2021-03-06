/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.trifork.riak;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author russell
 *
 */
@Test(groups = "unit")
public class TestRequestMeta {
    
    public void builderIsPopulatedFromSetterValues() {
        final int dw = 2;
        final int w = 3;
        final boolean returnBody = true;
        final String contentType = "application/json";
        final RPB.RpbPutReq.Builder builder = RPB.RpbPutReq.newBuilder();
        
        RequestMeta requestMeta = new RequestMeta();
        requestMeta.dw(dw).w(w).returnBody(returnBody).contentType(contentType);
        
        assertEquals(contentType,  requestMeta.getContentType().toStringUtf8());
        
        requestMeta.preparePut(builder);
        
        assertEquals(dw, builder.getDw());
        assertEquals(w, builder.getW());
        assertEquals(returnBody, builder.getReturnBody());
    }

}
