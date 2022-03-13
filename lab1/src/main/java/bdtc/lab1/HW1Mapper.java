package bdtc.lab1;

import com.google.gson.Gson;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;


/**
 * Маппер: выдает пары ключ-значение: зона - кол-во кликов
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private final ArrayList<ZoneInfo> zoneInfos = new ArrayList<>();

    private static class CsvLineFormat {
        private final static int X = 0;
        private final static int Y = 1;
        private final static int TIMESTAMP = 2;
        private final static int DATETIME = 3;
    }

    /*
     * Предварительное чтение координат зон из конфигурации
     */
    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String[] zonesNames = conf.getStrings("zones.names");

        Gson gson = new Gson();

        for (String zoneName: zonesNames) {
            String serializedInstance = conf.get(zoneName);
            ZoneInfo zoneInfo = gson.fromJson(serializedInstance, ZoneInfo.class);
            zoneInfos.add(zoneInfo);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try (CSVReader reader = new CSVReader(new StringReader(value.toString()))) {
            String[] line = reader.readNext();

            Point point = new Point(Integer.parseInt(line[CsvLineFormat.X]), Integer.parseInt(line[CsvLineFormat.Y]));

            for (ZoneInfo zoneInfo: zoneInfos) {
                if (isPointInZone(point, zoneInfo)) {
                    word.set(zoneInfo.getName());
                    context.write(word, one);
                    return;
                }
            }
            context.getCounter(CounterType.UNKNOWN_ZONE).increment(1);
        }
        catch (CsvException | NumberFormatException ignored) {}
    }

    private Boolean isPointInZone(Point point, ZoneInfo zoneInfo) {
        int height = zoneInfo.getEndPoint().y - zoneInfo.getStartPoint().y;
        int width = zoneInfo.getEndPoint().x - zoneInfo.getStartPoint().x;

        Dimension dimension = new Dimension(width, height);
        Rectangle rectangle = new Rectangle(zoneInfo.getStartPoint(), dimension);

        return rectangle.contains(point);
    }
}
