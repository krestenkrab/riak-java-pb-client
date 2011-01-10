package com.trifork.riak;

import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.FileAssert.fail;

/**
 * Verifies that Riak is running.
 *
 * @author reiks, Jan 5, 2011
 */
@Test(groups = "db-available")
public class RiakRunningTest {
    
    public void testIsRunning() {
        try
        {
            RiakClient client = new RiakClient("127.0.0.1");
            client.ping();
        }
        catch (IOException e)
        {
            fail("Unable to connect to Riak (host: 127.0.0.1, port: " + RiakConnection.DEFAULT_RIAK_PB_PORT + ")");
        }
    }
}