/**
 * Sample file using the batch RiakClient#store(RiakObject[], ...) API
 */

package com.trifork.riak;

import java.io.IOException;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

@Test(groups = "db", dependsOnMethods = "testIsRunning", sequential = true)
public class BatchPushLotsOfDataTest {

    public void testBatchPushingLotsofData() throws InterruptedException, IOException {
        final RiakClient riak = new RiakClient("127.0.0.1");

		long before = System.currentTimeMillis();

		Thread t1 = new Thread() {
			public void run() {
				try {
					insert_batch(riak, 1);
				} catch (IOException e) {
					e.printStackTrace();
				}
			};
		};

		Thread t2 = new Thread() {
			public void run() {
				try {
					insert_batch(riak, 2);
				} catch (IOException e) {
					e.printStackTrace();
				}
			};
		};

		t1.start(); t2.start();

		t1.join(); t2.join();


		long after = System.currentTimeMillis();

		System.out.println("TOTAL TIME: "+(1.0*(after-before)/1000.0)+"s");
    }

	private static void insert_batch(RiakClient riak, int prefix) throws IOException {
		byte[] val = new byte[419];
		ByteString value = ByteString.copyFrom(val);


		for (int i = 0; i < 100; i++) {

			long before2 = System.currentTimeMillis();

			String cpr = "" + (prefix*1000000000+i);
			ByteString cpr_key = ByteString.copyFromUtf8(cpr);

			RiakObject[] many = new RiakObject[1000];

			for (int rid = 0; rid < 1000; rid++)  {

				ByteString rid_key = ByteString.copyFromUtf8("" + (200000+i));

				many[rid] = new RiakObject(cpr_key, rid_key, value);

			}

			riak.store(many, null);

			long after2 = System.currentTimeMillis();

			System.out.println(prefix+": "+i+" x 1000 INSERTS: "+(after2-before2)+"ms");

		}
	}

}
