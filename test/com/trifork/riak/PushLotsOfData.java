package com.trifork.riak;

import java.io.IOException;

import com.google.protobuf.ByteString;

public class PushLotsOfData {

	
	
	public static void main(String[] args) throws IOException {
		
		RiakClient riak = new RiakClient("127.0.0.1");
		
		long before = System.currentTimeMillis();
		
		
		byte[] val = new byte[419];
		ByteString value = ByteString.copyFrom(val);
		
		
		for (int i = 0; i < 100; i++) {
			
			long before2 = System.currentTimeMillis();

			String cpr = "" + (1000000000+i);
			ByteString cpr_key = ByteString.copyFromUtf8(cpr);
			
			for (int rid = 0; rid < 1000; rid++)  {
				
				ByteString rid_key = ByteString.copyFromUtf8("" + (200000+i));
				
				RiakObject ro = new RiakObject(cpr_key, rid_key, value);
				riak.store(ro);
				
			}
			
			long after2 = System.currentTimeMillis();
			
			System.out.println(""+i+" x 1000 INSERTS: "+(after2-before2)+"ms");

		}
		
		
		long after = System.currentTimeMillis();
		
		System.out.println("TOTAL TIME: "+(1.0*(after-before)/1000.0)+"s");
	}
	
}
