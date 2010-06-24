/**
 * Sample file using the RiakClient#store(RiakObject, ...) API
 */

package com.trifork.riak_http;

import java.io.IOException;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;

public class HTTPPushLotsOfData {

	
	
	public static void main(String[] args) throws IOException {
		
		RiakClient riak = new RiakClient("http://127.0.0.1:8098/riak");
		
		long before = System.currentTimeMillis();
		
		
		byte[] val = new byte[419];
		
		
		for (int i = 0; i < 100; i++) {
			
			long before2 = System.currentTimeMillis();

			String cpr = "" + (1000000000+i);
			String cpr_key = cpr;
			
			for (int rid = 0; rid < 1000; rid++)  {
				
				String rid_key = "" + (200000+i);
				
				RiakObject ro = new RiakObject(riak, cpr_key, rid_key, val);
				riak.store(ro);
				
			}
			
			long after2 = System.currentTimeMillis();
			
			System.out.println(""+i+" x 1000 INSERTS: "+(after2-before2)+"ms");

		}
		
		
		long after = System.currentTimeMillis();
		
		System.out.println("TOTAL TIME: "+(1.0*(after-before)/1000.0)+"s");
	}
	
}
