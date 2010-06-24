package com.trifork.riak;

import java.io.IOException;

import com.trifork.riak.RPB.RpbErrorResp;

public class RiakError extends IOException {

	public RiakError(RpbErrorResp err) {
		super(err.getErrmsg().toStringUtf8());
	}

}
