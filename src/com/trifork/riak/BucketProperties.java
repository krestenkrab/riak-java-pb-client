package com.trifork.riak;

import com.trifork.riak.RPB.RpbBucketProps;
import com.trifork.riak.RPB.RpbGetBucketResp;
import com.trifork.riak.RPB.RpbBucketProps.Builder;

public class BucketProperties {

	private Boolean allowMult;
	private Integer nValue;

	public void init(RpbGetBucketResp resp) {
		if (resp.hasProps()) {
			
			RpbBucketProps props = resp.getProps();
			if (props.hasAllowMult()) {
				allowMult = Boolean.valueOf(props.getAllowMult());
			}
			if (props.hasNVal()) {
				nValue = new Integer(props.getNVal());
			}
		}
	}
	
	public Boolean getAllowMult() {
		return allowMult;
	}
	
	public Integer getNValue() {
		return nValue;
	}

	RpbBucketProps build() {
		Builder builder = RpbBucketProps.newBuilder();
		if (allowMult != null) {
			builder.setAllowMult(allowMult);
		}
		if (nValue != null) {
			builder.setNVal(nValue);
		}
		return builder.build();
	}

}
