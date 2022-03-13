import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import bdtc.lab1.TemperatureIntervalInfo;
import bdtc.lab1.ZoneInfo;
import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String testStringValue = "150,150,1645978416142,2022-02-27 19:13:36.141929";

    private final String zone1Name = "zone1";
    private final String zone2Name = "zone2";

    private final String lowInterval = "low";
    private final String mediumInterval = "medium";
    private final String highInterval = "high";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();

        Gson gson = new Gson();
        String serializedZone1 = gson.toJson(new ZoneInfo(zone1Name, new Point(50, 50), new Point(200, 200)));
        String serializedZone2 = gson.toJson(new ZoneInfo(zone2Name, new Point(201, 0), new Point(400, 400)));

        String serializedInterval1 = gson.toJson(new TemperatureIntervalInfo(lowInterval, 0L, 100L));
        String serializedInterval2 = gson.toJson(new TemperatureIntervalInfo(mediumInterval, 100L, 1000L));
        String serializedInterval3 = gson.toJson(new TemperatureIntervalInfo(highInterval, 1000L, 10000L));


        mapDriver = MapDriver.newMapDriver(mapper);

        mapDriver.getConfiguration().setStrings("zones.names", zone1Name, zone2Name);
        mapDriver.getConfiguration().set(zone1Name, serializedZone1);
        mapDriver.getConfiguration().set(zone2Name, serializedZone2);

        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        reduceDriver.getConfiguration().setStrings("temperatures.names", lowInterval, mediumInterval, highInterval);
        reduceDriver.getConfiguration().set(lowInterval, serializedInterval1);
        reduceDriver.getConfiguration().set(mediumInterval, serializedInterval2);
        reduceDriver.getConfiguration().set(highInterval, serializedInterval3);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        mapReduceDriver.getConfiguration().set("zones.names", zone1Name, zone2Name);
        mapReduceDriver.getConfiguration().set(zone1Name, serializedZone1);
        mapReduceDriver.getConfiguration().set(zone2Name, serializedZone2);

        mapReduceDriver.getConfiguration().setStrings("temperatures.names", lowInterval, mediumInterval, highInterval);
        mapReduceDriver.getConfiguration().set(lowInterval, serializedInterval1);
        mapReduceDriver.getConfiguration().set(mediumInterval, serializedInterval2);
        mapReduceDriver.getConfiguration().set(highInterval, serializedInterval3);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testStringValue))
                .withOutput(new Text(zone1Name), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text(zone2Name), values)
                .withOutput(new Text(zone2Name + " (" + lowInterval + ")"), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testMediumTemperature() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        for (int i = 0; i < 500; i++)
            values.add(new IntWritable(1));

        reduceDriver
                .withInput(new Text(zone2Name), values)
                .withOutput(new Text(zone2Name + " (" + mediumInterval + ")"), new IntWritable(500))
                .runTest();
    }

    @Test
    public void testHighTemperature() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        for (int i = 0; i < 5_000; i++)
            values.add(new IntWritable(1));

        reduceDriver
                .withInput(new Text(zone2Name), values)
                .withOutput(new Text(zone2Name + " (" + highInterval + ")"), new IntWritable(5_000))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testStringValue))
                .withInput(new LongWritable(), new Text(testStringValue))
                .withOutput(new Text(zone1Name + " (" + lowInterval + ")"), new IntWritable(2))
                .runTest();
    }
}
