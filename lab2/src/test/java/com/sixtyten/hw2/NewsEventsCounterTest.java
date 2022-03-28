package com.sixtyten.hw2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.sixtyten.hw2.NewsEventsCounter.countNewsEvents;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NewsEventsCounterTest {

    private final SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("Java Spark SQL hw2 Tests")
            .getOrCreate();

    private static final Map<Integer, String> newsConfig = Stream.of(new String[][]{
            {"1", "open and read"},
            {"2", "open to preview"},
            {"3", "no action"}
    }).collect(Collectors.toMap(data -> Integer.valueOf(data[0]), data -> data[1]));

    private Dataset<Row> testDataset;

    /**
     * Сравнение результата, полученных из входных данных файла src/test/resources/test1.csv
     * и содержимого src/test/resources/result1.csv
     */
    @Test
    public void testSimpleDataset() {
        testDataset = ss.read().csv("src/test/resources/test1.csv");
        testDataset = DatasetColumnsRenamer.renameColumns(testDataset);

        Dataset<Row> result = countNewsEvents(testDataset, new HashMap<>(newsConfig));
        result.show();

        Dataset<Row> trueResult = ss.read()
                .format("csv")
                .option("header", true)
                .load("src/test/resources/result1.csv");

        compareTwoDatasets(trueResult, result);
    }

    /**
     * Тест на пустые входные данные
     */
    @Test
    public void testEmptyDataSet() {
        StructType inputStructType = new StructType()
                .add(new StructField("news_id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("user_id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("type", DataTypes.StringType, true, Metadata.empty()));

        testDataset = ss.read().schema(inputStructType).csv(ss.emptyDataset(Encoders.STRING()));

        Dataset<Row> result = countNewsEvents(testDataset, new HashMap<>(newsConfig));

        StructType outputStructType = new StructType()
                .add(new StructField("news_id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("event_type", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("count", DataTypes.StringType, true, Metadata.empty()));

        Dataset<Row> trueEmptyDataset = ss.read().schema(outputStructType).csv(ss.emptyDataset(Encoders.STRING()));

        compareTwoDatasets(trueEmptyDataset, result);
    }

    private void compareTwoDatasets(Dataset<Row> trueDataset, Dataset<Row> resultDataset) {
        assertEquals(trueDataset.select("news_id").collectAsList(), resultDataset.select("news_id").collectAsList());
        assertEquals(trueDataset.select("event_type").collectAsList(), resultDataset.select("event_type").collectAsList());

        List<String> trueCount = trueDataset.select("count").collectAsList().stream().map(Row::toString).collect(
                Collectors.toList()
        );

        List<String> resultCount = resultDataset.select("count").collectAsList().stream().map(Row::toString).collect(
                Collectors.toList()
        );

        assertEquals(trueCount, resultCount);
    }
}
