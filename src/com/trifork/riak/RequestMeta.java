package com.trifork.riak;

public class RequestMeta {

	Boolean returnBody;
	Integer writeQuorum;
	Integer durableWriteQuorum;
	
	public RequestMeta() {
	}
	
	public void preparePut(RPB.RpbPutReq.Builder builder) {

		if (returnBody != null) {
			builder.setReturnBody(returnBody.booleanValue());
		}
		
		if (writeQuorum != null) {
			builder.setW(writeQuorum.intValue());
		}
		
		if (durableWriteQuorum != null) {
			builder.setDw(durableWriteQuorum.intValue());
		}		
	}

	RequestMeta returnBody(boolean ret) {
		returnBody = Boolean.valueOf(ret);
		return this;
	}

	RequestMeta w(int w) {
		w = new Integer(w);
		return this;
	}

	RequestMeta dw(int dw) {
		dw = new Integer(dw);
		return this;
	}
}
