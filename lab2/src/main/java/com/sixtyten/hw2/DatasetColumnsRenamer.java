package com.sixtyten.hw2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Переименование колонок таблицы, тк предполагается считывание
 * файла без header-а
 */
public class DatasetColumnsRenamer {

    private static final Map<String, String> columnNames = Stream.of(new String[][]{
            {"_c0", "news_id"},
            {"_c1", "user_id"},
            {"_c2", "timestamp"},
            {"_c3", "type"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    public static Dataset<Row> renameColumns(Dataset<Row> df) {
        for (String key : columnNames.keySet()) {
            df = df.withColumnRenamed(key, columnNames.get(key));
        }
        return df;
    }
}
