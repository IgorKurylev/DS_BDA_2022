package com.sixtyten.hw2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

@Slf4j
public class NewsEventsCounter {

    public static Dataset<Row> countNewsEvents(Dataset<Row> inputDataset, HashMap<Integer, String> newsTypesConfig) {

        Dataset<Row> groupedAndCounted = inputDataset.groupBy("news_id", "type").count();
        Dataset<Row> resultQuery = groupedAndCounted.select(
                col("news_id"),
                when(col("type").equalTo("1"), newsTypesConfig.get(1))
                        .when(col("type").equalTo("2"), newsTypesConfig.get(2))
                        .otherwise(newsTypesConfig.get(3)).as("event_type"),
                col("count")
        );

        resultQuery.show();
        return resultQuery;
    }
}
