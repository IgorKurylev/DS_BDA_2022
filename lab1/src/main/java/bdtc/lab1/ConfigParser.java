package bdtc.lab1;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.awt.*;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

/**
 * Парсер файлов конфигурации (зон и температурных интервалов)
 */
public class ConfigParser {

    public static void readAndSetZones(Path configPath, Configuration conf) {
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(String.valueOf(new Path(configPath.getName() + "/zones.json")))) {
            JSONObject zones = (JSONObject) jsonParser.parse(reader);
            Set<String> keys = zones.keySet();

            conf.setStrings("zones.names", keys.toArray(new String[0]));

            Gson gson = new Gson();
            for (String key : keys) {
                JSONArray zonePoints = (JSONArray) zones.get(key);
                JSONArray jsonStartPoint = (JSONArray) zonePoints.get(0);
                JSONArray jsonEndPoint = (JSONArray) zonePoints.get(1);


                Point startPoint = new Point(Math.toIntExact((Long) jsonStartPoint.get(0)), Math.toIntExact((Long) jsonStartPoint.get(1)));
                Point endPoint = new Point(Math.toIntExact((Long) jsonEndPoint.get(0)), Math.toIntExact((Long) jsonEndPoint.get(1)));

                ZoneInfo zoneInfo = new ZoneInfo(key, startPoint, endPoint);
                conf.set(key, gson.toJson(zoneInfo));
            }
        } catch (IOException e) {
            throw new RuntimeException("zones.json not found");
        } catch (ParseException e) {
            throw new RuntimeException("Incorrect zones.json format");
        }
    }

    public static void readAndSetTemperatures(Path configPath, Configuration conf) {
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(configPath.getName() + "/temperatures.json")) {
            JSONObject temperatures = (JSONObject) jsonParser.parse(reader);
            Set<String> keys = temperatures.keySet();

            conf.setStrings("temperatures.names", keys.toArray(new String[0]));

            Gson gson = new Gson();
            for (String key: keys) {
                JSONArray temperaturesInterval = (JSONArray) temperatures.get(key);
                Long startValue = (Long) temperaturesInterval.get(0);
                Long endValue = (Long) temperaturesInterval.get(1);

                TemperatureIntervalInfo temperatureIntervalInfo = new TemperatureIntervalInfo(key, startValue, endValue);
                conf.set(key, gson.toJson(temperatureIntervalInfo));
            }


        } catch (IOException e) {
            throw new RuntimeException("temperatures.json not found");
        } catch (ParseException e) {
            throw new RuntimeException("Incorrect temperatures.json format");
        }
    }
}
