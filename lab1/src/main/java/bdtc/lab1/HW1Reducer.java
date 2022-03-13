package bdtc.lab1;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final ArrayList<TemperatureIntervalInfo> temperatureIntervalInfos = new ArrayList<>();
    private final String UNKNOWN_INTERVAL = "unknown_temperature";

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String[] temperaturesIntervalsNames = conf.getStrings("temperatures.names");

        Gson gson = new Gson();

        for (String temperatureIntervalName : temperaturesIntervalsNames) {
            String serializedInstance = conf.get(temperatureIntervalName);
            TemperatureIntervalInfo temperatureIntervalInfo = gson.fromJson(serializedInstance, TemperatureIntervalInfo.class);
            temperatureIntervalInfos.add(temperatureIntervalInfo);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int clickCounts = 0;
        while (values.iterator().hasNext()) {
            clickCounts += values.iterator().next().get();
        }
        String interval = "";
        for (TemperatureIntervalInfo temperatureIntervalInfo: temperatureIntervalInfos) {
            if (isTemperatureInInterval(clickCounts, temperatureIntervalInfo))
                interval = temperatureIntervalInfo.getName();
        }
        if (interval.isEmpty())
            interval = UNKNOWN_INTERVAL;
        context.write(new Text(key + " (" + interval + ")"), new IntWritable(clickCounts));
    }

    private Boolean isTemperatureInInterval(int clickCounts, TemperatureIntervalInfo temperatureIntervalInfo) {
        return temperatureIntervalInfo.getStart() <= clickCounts && clickCounts < temperatureIntervalInfo.getEnd();
    }
}
