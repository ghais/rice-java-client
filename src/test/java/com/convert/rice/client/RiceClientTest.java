package com.convert.rice.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.convert.rice.TimeSeries;
import com.convert.rice.client.protocol.Response.GetResult;
import com.convert.rice.client.protocol.Response.GetResult.Metric;
import com.convert.rice.client.protocol.Response.IncResult;
import com.convert.rice.hbase.HBaseTimeSeries;
import com.convert.rice.server.protobuf.RiceProtoBufRpcServer;
import com.google.common.base.Supplier;

public class RiceClientTest {

    private static HBaseTestingUtility testUtil;

    private static HTablePool pool;

    private static final String type = "type";

    private static HBaseAdmin admin;

    private static RiceProtoBufRpcServer server;

    private static Configuration conf;

    @BeforeClass
    public static void setup() throws Exception {
        testUtil = new HBaseTestingUtility();
        testUtil.startMiniCluster();
        pool = new HTablePool(testUtil.getConfiguration(), 10);
        admin = testUtil.getHBaseAdmin();
        conf = testUtil.getConfiguration();
        final HBaseTimeSeries hBaseTimeSeries = new HBaseTimeSeries(conf, pool);
        hBaseTimeSeries.checkOrCreateTable(admin, type);

        server = new RiceProtoBufRpcServer(7654, new Supplier<TimeSeries>() {

            @Override
            public TimeSeries get() {
                return hBaseTimeSeries;
            }
        });
        server.startAndWait();
        System.out.println("Started");

    }

    @AfterClass
    public static void after() throws IOException, InterruptedException, ExecutionException {
        server.stop().get();
        new HBaseTimeSeries(conf, pool).deleteTable(admin, type);
        pool.close();
        testUtil.shutdownMiniCluster();
    }

    @Test
    public void testInc() throws Exception {
        @SuppressWarnings("serial")
        Map<String, Long> metrics = new HashMap<String, Long>() {

            {
                put("a", 10L);
                put("b", 20L);
            }
        };
        String key = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        RiceClient client = new RiceClient("localhost", 7654);
        IncResult result = client.inc(type, key, metrics, timestamp).get();
        assertEquals(key, result.getKey());
        assertEquals(timestamp, result.getTimestamp());
        assertEquals(type, result.getType());

        client.close();
    }

    @Test
    public void testGet() throws Exception {
        @SuppressWarnings("serial")
        Map<String, Long> metrics = new HashMap<String, Long>() {

            {
                put("a", 10L);
                put("b", 20L);
            }
        };
        String key = UUID.randomUUID().toString();
        // 5 hour difference between start and end since the second04:00:01 will cause the end point to be 04:00:00
        long start = new DateTime(2012, 01, 01, 00, 00, 00).getMillis();
        long end = new DateTime(2012, 01, 01, 04, 00, 01).getMillis();

        RiceClient client = new RiceClient("localhost", 7654);

        // increment twice.
        client.inc(type, key, metrics, start).get();
        client.inc(type, key, metrics, start).get();

        GetResult result = client.get(type, key, start, end, 60 * 60 * 1000).get();
        assertEquals(key, result.getKey());
        assertEquals(metrics.size(), result.getMetricsCount());
        Map<String, Metric> metricsMap = new HashMap<String, Metric>(metrics.size());
        for (Metric m : result.getMetricsList()) {
            metricsMap.put(m.getName(), m);
        }
        Metric metricA = metricsMap.get("a");
        Metric metricB = metricsMap.get("b");

        assertNotNull(metricA);
        assertNotNull(metricB);

        assertEquals(5, metricA.getPointsCount());
        assertEquals(5, metricB.getPointsCount());

        assertEquals(start,
                metricA.getPoints(0).getStart());
        assertEquals(start,
                metricB.getPoints(0).getStart());

        assertEquals(20L, metricA.getPoints(0).getValue());
        assertEquals(40L, metricB.getPoints(0).getValue());

        client.close();
    }

}
