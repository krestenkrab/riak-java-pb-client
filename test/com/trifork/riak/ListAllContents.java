package com.trifork.riak;

import java.io.IOException;
import java.util.Map;

import com.google.protobuf.ByteString;

public class ListAllContents {

	public static void main(String[] args) throws IOException {
		
		RiakClient client = new RiakClient("127.0.0.1");
		Map<String, String> si = client.getServerInfo();
		
		System.out.println("connected to: "+si);
		
		ByteString[] buckets = client.listBuckets();
		for (ByteString bucket : buckets) {
			
			System.out.println("=bucket "+bucket.toStringUtf8());
			
			ByteString[] keys = client.listKeys(bucket);
			for (ByteString key : keys) {

				System.out.println("==key "+key.toStringUtf8());

				RiakObject[] ros = client.fetch(bucket, key);
				for (RiakObject ro : ros) {
					
					System.out.println("==="+ro.toString());
					
				}
			}
		}
		
		
	}
	
	
}
