package com.trifork.riak;

import java.util.List;

import com.google.protobuf.ByteString;
import com.trifork.riak.RPB.RpbLink;
import com.trifork.riak.RPB.RpbLink.Builder;

public class RiakLink {

	private ByteString bucket;
	private ByteString key;
	private ByteString tag;

	RiakLink(RpbLink rpbLink) {
		this.bucket = rpbLink.getBucket();
		this.key = rpbLink.getKey();
		this.tag = rpbLink.getTag();
	}

	public static RiakLink[] decode(List<RpbLink> list) {
		RiakLink[] res = new RiakLink[list.size()];
		for (int i = 0; i < res.length; i++) {
			res[i] = new RiakLink(list.get(i));
		}	
		return res;
	}

	public RpbLink build() {
		Builder b = RpbLink.newBuilder();
		
		if (bucket != null)
			b.setBucket(bucket);
		
		if (key != null)
			b.setKey(key);
		
		if (tag != null) 
			b.setTag(tag);

		return b.build();
	}

}
