## Java Client API for Riak based on the Protocol Buffers API

This is an alternative to
[riak-java-client](http://hg.basho.com/riak-java-client/) which offers
significantly better performance because you avoid encoding and
decoding content into the HTTP protocol.  On my dev machine I see ~
10x performance improvement; so that's worth taking.

The API is reasonably complete with what you can do in the Protocol
Buffers API.  

    RiakClient riak = new RiakClient("127.0.0.1", 8087);
    byte[] data = new byte[] { 1, 2, 3, 4 };
    RiakObject ro = new RiakObject("Bucket", "Key", 
                                   ByteString.copyFrom(data));
    riak.store(ro);

Some of the API uses `com.google.protobuf.ByteString` which represents
a chunk of (uninterpreted) bytes.  Much of the API supports both
`java.lang.String` and `ByteString`; sometimes you have to navigate
from one kind of String to another using
e.g. `ByteString.copyFromUtf8(String)` and
`ByteString.toStringUtf8()`; but this lets you have keys that are
arbitrary byte sequences in stead of just strings.  You'll find that
class in `lib/protobuf-java-2.3.0.jar` which also needs to be in your
class path for the library to work.

Here is a complete program to list the entire contents of a riak
store. 

    RiakClient client = new RiakClient("127.0.0.1");
    ByteString[] buckets = client.listBuckets();
    for (ByteString bucket : buckets) {
        System.out.println("=bucket "+bucket.toStringUtf8());
        KeySource keys = client.listKeys(bucket);
        for (ByteString key : keys) {
            System.out.println("==key "+key.toStringUtf8());
            RiakObject[] ros = client.fetch(bucket, key);
            for (RiakObject ro : ros) {
                System.out.println("==="+ro.toString());
            }
        }
        keys.close();
    }

The methods `RiakClient.listKeys` and `RiakClient.mapReduce` return
streaming handlers that should be closed explicitly (unless you run
it to completion in which case it closes itself).

If you set a clientID `RiakClient.setClientID`; that client ID is used 
for all connections coming from the same client.  `RiakClient` is 
"thread safe" so you can use it from several threads at the same time.

### Bulk Insert

The method `RiakClient.store(RiakObject[], RequestMeta)` allows
efficient bulk insertion (connection request/responses are handled
asynchroneously underneath the covers).  To run even faster, you can 
choose to run this in several threads (resulting in multiple connections).


## License / Copyright

If you use this library, please be so kind to credit Trifork
approproately in your system documentation.


    A Java Client API for Riak based on the Protocol Buffers API

    Copyright (C) 2010 Trifork A/S

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
