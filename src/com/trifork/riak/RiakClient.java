/**
 * This file is part of riak-java-pb-client 
 *
 * Copyright (c) 2010 by Trifork
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.trifork.riak;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import org.json.JSONObject;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.trifork.riak.RPB.RpbDelReq;
import com.trifork.riak.RPB.RpbErrorResp;
import com.trifork.riak.RPB.RpbGetClientIdResp;
import com.trifork.riak.RPB.RpbGetReq;
import com.trifork.riak.RPB.RpbGetResp;
import com.trifork.riak.RPB.RpbGetServerInfoResp;
import com.trifork.riak.RPB.RpbListBucketsResp;
import com.trifork.riak.RPB.RpbListKeysResp;
import com.trifork.riak.RPB.RpbMapRedReq;
import com.trifork.riak.RPB.RpbMapRedResp;
import com.trifork.riak.RPB.RpbPutReq;
import com.trifork.riak.RPB.RpbPutResp;
import com.trifork.riak.RPB.RpbSetClientIdReq;
import com.trifork.riak.RPB.RpbMapRedReq.Builder;
import com.trifork.riak.mapreduce.MapReduceResponse;

public class RiakClient {

	public static final int MSG_ErrorResp = 0;
	public static final int MSG_PingReq = 1;
	public static final int MSG_PingResp = 2;
	public static final int MSG_GetClientIdReq = 3;
	public static final int MSG_GetClientIdResp = 4;
	public static final int MSG_SetClientIdReq = 5;
	public static final int MSG_SetClientIdResp = 6;
	public static final int MSG_GetServerInfoReq = 7;
	public static final int MSG_GetServerInfoResp = 8;
	public static final int MSG_GetReq = 9;
	public static final int MSG_GetResp = 10;
	public static final int MSG_PutReq = 11;
	public static final int MSG_PutResp = 12;
	public static final int MSG_DelReq = 13;
	public static final int MSG_DelResp = 14;
	public static final int MSG_ListBucketsReq = 15;
	public static final int MSG_ListBucketsResp = 16;
	public static final int MSG_ListKeysReq = 17;
	public static final int MSG_ListKeysResp = 18;
	public static final int MSG_GetBucketReq = 19;
	public static final int MSG_GetBucketResp = 20;
	public static final int MSG_SetBucketReq = 21;
	public static final int MSG_SetBucketResp = 22;
	public static final int MSG_MapRedReq = 23;
	public static final int MSG_MapRedResp = 24;

	private static final int DEFAULT_RIAK_PB_PORT = 8087;
	private static final RiakObject[] NO_RIAK_OBJECTS = new RiakObject[0];
	private static final ByteString[] NO_BYTE_STRINGS = new ByteString[0];
	private static final MapReduceResponse[] NO_MAP_REDUCE_RESPONSES = new MapReduceResponse[0];
	private Socket sock;
	private DataOutputStream dout;
	private DataInputStream din;
	private String node;
	private String serverVersion;

	public RiakClient(String host) throws IOException {
		this(host, DEFAULT_RIAK_PB_PORT);
	}

	public RiakClient(String host, int port) throws IOException {
		this(InetAddress.getByName(host), port);
	}

	public RiakClient(InetAddress addr, int port) throws IOException {
		sock = new Socket(addr, port);
		
		sock.setSendBufferSize(1024 * 200);
		
		dout = new DataOutputStream(new BufferedOutputStream(sock
				.getOutputStream(), 1024 * 200));
		din = new DataInputStream(
				new BufferedInputStream(sock.getInputStream(), 1024 * 200));

		ping();

		// prepareClientID();

		getServerInfo();
	}

	/**
	 * helper method to use a reasonable default client id
	 * 
	 * @throws IOException
	 */
	public void prepareClientID() throws IOException {
		Preferences prefs = Preferences.userNodeForPackage(RiakClient.class);

		String clid = prefs.get("client_id", null);
		if (clid == null) {
			SecureRandom sr;
			try {
				sr = SecureRandom.getInstance("SHA1PRNG");
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
			byte[] data = new byte[6];
			sr.nextBytes(data);
			clid = Base64Coder.encodeLines(data);
			prefs.put("client_id", clid);
			try {
				prefs.flush();
			} catch (BackingStoreException e) {
				throw new IOException(e);
			}
		}

		setClientID(clid);
	}

	public void ping() throws IOException {
		send(MSG_PingReq);
		receive_code(MSG_PingResp);
	}

	public void setClientID(String id) throws IOException {
		setClientID(ByteString.copyFromUtf8(id));
	}

	// /////////////////////

	public void setClientID(ByteString id) throws IOException {
		RpbSetClientIdReq req = RPB.RpbSetClientIdReq.newBuilder().setClientId(
				id).build();
		send(MSG_SetClientIdReq, req);
		receive_code(MSG_SetClientIdResp);
	}

	public String getClientID() throws IOException {
		send(MSG_GetClientIdReq);
		byte[] data = receive(MSG_GetClientIdResp);
		if (data == null)
			return null;
		RpbGetClientIdResp res = RPB.RpbGetClientIdResp.parseFrom(data);
		return res.getClientId().toStringUtf8();
	}

	public Map<String, String> getServerInfo() throws IOException {
		send(MSG_GetServerInfoReq);
		byte[] data = receive(MSG_GetServerInfoResp);
		if (data == null)
			return Collections.emptyMap();
		
		RpbGetServerInfoResp res = RPB.RpbGetServerInfoResp.parseFrom(data);
		if (res.hasNode()) {
			this.node = res.getNode().toStringUtf8();
		}
		if (res.hasServerVersion()) {
			this.serverVersion = res.getServerVersion().toStringUtf8();
		}
		Map<String, String> result = new HashMap<String, String>();
		result.put("node", node);
		result.put("server_version", serverVersion);
		return result;
	}

	// /////////////////////

	public RiakObject[] fetch(String bucket, String key, int readQuorum)
			throws IOException {
		return fetch(ByteString.copyFromUtf8(bucket), ByteString
				.copyFromUtf8(key), readQuorum);
	}

	public RiakObject[] fetch(ByteString bucket, ByteString key, int readQuorum)
			throws IOException {
		RpbGetReq req = RPB.RpbGetReq.newBuilder().setBucket(bucket)
				.setKey(key).setR(readQuorum).build();

		send(MSG_GetReq, req);
		return process_fetch_reply(bucket, key);

	}

	public RiakObject[] fetch(String bucket, String key) throws IOException {
		return fetch(ByteString.copyFromUtf8(bucket), ByteString
				.copyFromUtf8(key));
	}

	public RiakObject[] fetch(ByteString bucket, ByteString key)
			throws IOException {
		RpbGetReq req = RPB.RpbGetReq.newBuilder().setBucket(bucket)
				.setKey(key).build();

		send(MSG_GetReq, req);
		return process_fetch_reply(bucket, key);
	}

	private RiakObject[] process_fetch_reply(ByteString bucket, ByteString key)
			throws IOException, InvalidProtocolBufferException {
		byte[] rep = receive(MSG_GetResp);

		if (rep == null) {
			return NO_RIAK_OBJECTS;
		}

		RpbGetResp resp = RPB.RpbGetResp.parseFrom(rep);
		int count = resp.getContentCount();
		RiakObject[] out = new RiakObject[count];
		ByteString vclock = resp.getVclock();
		for (int i = 0; i < count; i++) {
			out[i] = new RiakObject(vclock, bucket, key, resp.getContent(i));
		}
		return out;
	}

	// /////////////////////

	public ByteString[] store(RiakObject[] values, RequestMeta meta)
			throws IOException {

		BulkReader reader = new BulkReader(values.length);
		Thread worker = new Thread(reader);
		worker.start();

		for (int i = 0; i < values.length; i++) {
			RiakObject value = values[i];

			RPB.RpbPutReq.Builder builder = RPB.RpbPutReq.newBuilder()
					.setBucket(value.getBucket()).setKey(value.getKey())
					.setContent(value.buildContent());

			if (value.getVclock() != null) {
				builder.setVclock(value.getVclock());
			}

			if (meta != null) {

				builder.setReturnBody(false);

				if (meta.writeQuorum != null) {
					builder.setW(meta.writeQuorum.intValue());
				}

				if (meta.durableWriteQuorum != null) {
					builder.setDw(meta.durableWriteQuorum.intValue());
				}
			}

			RpbPutReq req = builder.build();

			int len = req.getSerializedSize();
			dout.writeInt(len + 1);
			dout.write(MSG_PutReq);
			req.writeTo(dout);
		}

		dout.flush();

		try {
			worker.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return reader.vclocks;
	}

	class BulkReader implements Runnable {

		private ByteString[] vclocks;

		public BulkReader(int count) {
			this.vclocks = new ByteString[count];
		}

		@Override
		public void run() {

			try {
				for (int i = 0; i < vclocks.length; i++) {
					byte[] data = receive(MSG_PutResp);
					if (data != null) {
						RpbPutResp resp = RPB.RpbPutResp.parseFrom(data);
						vclocks[i] = resp.getVclock();
					}
				}
			} catch (IOException e) {
				// TODO
				e.printStackTrace();
			}

		}

	}

	public void store(RiakObject value) throws IOException {
		store(value, null);
	}

	public RiakObject[] store(RiakObject value, RequestMeta meta)
			throws IOException {

		RPB.RpbPutReq.Builder builder = RPB.RpbPutReq.newBuilder().setBucket(
				value.getBucket()).setKey(value.getKey()).setContent(
				value.buildContent());

		if (value.getVclock() != null) {
			builder.setVclock(value.getVclock());
		}

		if (meta != null) {
			meta.preparePut(builder);
		}

		send(MSG_PutReq, builder.build());
		byte[] r = receive(MSG_PutResp);

		if (r == null) {
			return NO_RIAK_OBJECTS;
		}
		
		RpbPutResp resp = RPB.RpbPutResp.parseFrom(r);

		RiakObject[] res = new RiakObject[resp.getContentsCount()];
		ByteString vclock = resp.getVclock();

		for (int i = 0; i < res.length; i++) {
			res[i] = new RiakObject(vclock, value.getBucket(), value.getKey(),
					resp.getContents(i));
		}

		return res;
	}

	// /////////////////////

	void delete(String bucket, String key, int rw) throws IOException {
		delete(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key),
				rw);
	}

	public void delete(ByteString bucket, ByteString key, int rw)
			throws IOException {
		RpbDelReq req = RPB.RpbDelReq.newBuilder().setBucket(bucket)
				.setKey(key).setRw(rw).build();

		send(MSG_DelReq, req);
		receive_code(MSG_DelResp);
	}

	void delete(String bucket, String key) throws IOException {
		delete(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key));
	}

	public void delete(ByteString bucket, ByteString key) throws IOException {
		RpbDelReq req = RPB.RpbDelReq.newBuilder().setBucket(bucket)
				.setKey(key).build();

		send(MSG_DelReq, req);
		receive_code(MSG_DelResp);
	}

	public ByteString[] listBuckets() throws IOException {

		send(MSG_ListBucketsReq);

		byte[] data = receive(MSG_ListBucketsResp);
		if (data == null) {
			return NO_BYTE_STRINGS;
		}

		RpbListBucketsResp resp = RPB.RpbListBucketsResp.parseFrom(data);
		ByteString[] out = new ByteString[resp.getBucketsCount()];
		for (int i = 0; i < out.length; i++) {
			out[i] = resp.getBuckets(i);
		}
		return out;
	}

	public BucketProperties getBucketProperties(ByteString bucket) throws IOException {
		
		send(MSG_GetBucketReq,
			 RPB.RpbGetBucketReq.newBuilder()
				.setBucket(bucket).build());
		
		byte[] data = receive(MSG_GetBucketResp);
		BucketProperties bp = new BucketProperties();
		if (data == null) {
			return bp;
		} 

		bp.init(RPB.RpbGetBucketResp.parseFrom(data));
		return bp;
	}
	
	public void setBucketProperties(ByteString bucket, BucketProperties props) throws IOException {
		
		RPB.RpbSetBucketReq req = RPB.RpbSetBucketReq.newBuilder()
				.setBucket(bucket)
				.setProps( props.build() ).build();
	
		send(MSG_SetBucketReq, req);
		receive_code(MSG_SetBucketResp);
	}
	
	
	// /////////////////////

	public ByteString[] listKeys(ByteString bucket) throws IOException {

		send(MSG_ListKeysReq, RPB.RpbListKeysReq.newBuilder().setBucket(bucket)
				.build());

		List<ByteString> keys = new ArrayList<ByteString>();

		RpbListKeysResp r;
		do {
			byte[] data = receive(MSG_ListKeysResp);
			if (data == null) {
				return NO_BYTE_STRINGS;
			}
			r = RPB.RpbListKeysResp.parseFrom(data);

			for (int i = 0; i < r.getKeysCount(); i++) {
				keys.add(r.getKeys(i));
			}

		} while (!r.hasDone() || r.getDone() == false);

		return keys.toArray(new ByteString[keys.size()]);
	}
	
	

	///////////////////////

	MapReduceResponse[] mapreduce(JSONObject obj) throws IOException {
		return mapreduce(ByteString.copyFromUtf8(obj.toString()), 
				new RequestMeta().contentType("application/json"));
	}

	public MapReduceResponse[] mapreduce(ByteString request, RequestMeta meta) throws IOException {
		
		ByteString contentType = meta.getContentType();
		if (contentType == null) {
			throw new IllegalArgumentException("no content type");
		}
		RpbMapRedReq req = RPB.RpbMapRedReq.newBuilder()
			.setRequest(request)
			.setContentType(meta.getContentType()).build();
		
		send(MSG_MapRedReq, req);
		byte[] data = receive(MSG_MapRedResp);
		if (data == null) {
			return NO_MAP_REDUCE_RESPONSES;
		} 

		List<MapReduceResponse> out = new ArrayList<MapReduceResponse>();
		RpbMapRedResp resp;
		do {
			resp = RPB.RpbMapRedResp.parseFrom(data);
			out.add(new MapReduceResponse(resp, contentType));
			
		} while (!resp.hasDone() || resp.getDone() == false); 
		
		return out.toArray(new MapReduceResponse[out.size()]);
	}
	

	///////////////////////

	private void send(int code, MessageLite req) throws IOException {
		int len = req.getSerializedSize();
		dout.writeInt(len + 1);
		dout.write(code);
		req.writeTo(dout);
		dout.flush();
	}

	private void send(int code) throws IOException {
		dout.writeInt(1);
		dout.write(code);
		dout.flush();
	}

	private byte[] receive(int code) throws IOException {
		int len = din.readInt();
		int get_code = din.read();

		if (code == MSG_ErrorResp) {
			RpbErrorResp err = com.trifork.riak.RPB.RpbErrorResp.parseFrom(din);
			throw new RiakError(err);
		}

		byte[] data = null;
		if (len > 1) {
			data = new byte[len - 1];
			din.readFully(data);
		}

		if (code != get_code) {
			throw new IOException("bad message code");
		}

		return data;
	}

	private void receive_code(int code) throws IOException, RiakError {
		int len = din.readInt();
		int get_code = din.read();
		if (code == MSG_ErrorResp) {
			RpbErrorResp err = com.trifork.riak.RPB.RpbErrorResp.parseFrom(din);
			throw new RiakError(err);
		}
		if (len != 1 || code != get_code) {
			throw new IOException("bad message code");
		}
	}
}
