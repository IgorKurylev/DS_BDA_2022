import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.ZoneInfo;
import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testUnknownZone = "-1,-1,1645978416142,2022-02-27 19:13:36.141929";
    private final String testStringValue = "150,150,1645978416142,2022-02-27 19:13:36.141929";

    private final String zone1Name = "zone1";
    private final String zone2Name = "zone2";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();

        Gson gson = new Gson();
        String serializedZone1 = gson.toJson(new ZoneInfo(zone1Name, new Point(50, 50), new Point(200, 200)));
        String serializedZone2 = gson.toJson(new ZoneInfo(zone2Name, new Point(201, 0), new Point(400, 400)));

        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.getConfiguration().set("zones.names", zone1Name, zone2Name);
        mapDriver.getConfiguration().set(zone1Name, serializedZone1);
        mapDriver.getConfiguration().set(zone2Name, serializedZone2);
    }

    @Test
    public void testMapperUnknownZone() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testUnknownZone))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.UNKNOWN_ZONE).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testStringValue))
                .withOutput(new Text(zone1Name), new IntWritable(1))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.UNKNOWN_ZONE).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testStringValue))
                .withInput(new LongWritable(), new Text(testUnknownZone))
                .withInput(new LongWritable(), new Text(testUnknownZone))
                .withOutput(new Text(zone1Name), new IntWritable(1))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.UNKNOWN_ZONE).getValue());
    }
}

