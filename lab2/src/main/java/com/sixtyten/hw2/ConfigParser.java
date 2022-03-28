package com.sixtyten.hw2;

import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * Считывание справочника по предоставленному пути
 */
public class ConfigParser {

    public static HashMap<Integer, String> parseNewsConfig(Path configPath) {
        JSONParser jsonParser = new JSONParser();

        HashMap<Integer, String> result = new HashMap<>();
        try (FileReader reader = new FileReader(String.valueOf(configPath))) {
            JSONObject newsTypes = (JSONObject) jsonParser.parse(reader);
            Set<String> keys = newsTypes.keySet();

            for (String key: keys)
                result.put(Integer.valueOf(key), (String) newsTypes.get(key));

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("news_types.json not found");
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("Incorrect news_types.json format");
        }

        return result;
    }
}
