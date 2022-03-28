package com.sixtyten.hw2;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

@Slf4j
public class MainApplication {

    private static String hdfsPrefix = "hdfs://127.0.0.1:9000";

    public static void main(String[] args) {

        if (args.length < 3) {
            throw new RuntimeException("Usage: ./gradlew run --args=\"<input_dir> <output_dir> <config_dir> <hdfs_prefix_opt>\"");
        }
        if (args.length == 4)
            hdfsPrefix = args[3];

        String inputDir = args[0];
        String outputDir = args[1];
        String configPath = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL hw2")
                .config("spark.master", "local")
                .getOrCreate();

        ArrayList<Path> filesPathList = new ArrayList<>();
        try {
            FileSystem fileSystem = FileSystem.get(new URI(hdfsPrefix), spark.sparkContext().hadoopConfiguration());
            Path hdfsPath = new Path(inputDir);

            for (FileStatus fileStatus : fileSystem.listStatus(hdfsPath)) {
                log.info("FileStatus: " + fileStatus.getPath());
                filesPathList.add(fileStatus.getPath());
            }

            Path outputDirectory = new Path(hdfsPrefix + outputDir);
            if (fileSystem.exists(outputDirectory)) {
                log.info("=== Deleting output directory ===");
                fileSystem.delete(outputDirectory, true);
            }
        } catch (IOException exc) {
            log.error("Error listing files from hdfs!");
            exc.printStackTrace();
            return;
        } catch (URISyntaxException e) {
            log.error("Invalid hdfs path!");
            e.printStackTrace();
        }

        Dataset<Row> df = spark.read().csv(filesPathList.get(0).toString());
        df = DatasetColumnsRenamer.renameColumns(df);
        df.show();

        HashMap<Integer, String> newsTypesConfig = ConfigParser.parseNewsConfig(new Path(configPath));

        log.info("========= COUNTING STARTED =========");
        Dataset<Row> groupedAndCounted = NewsEventsCounter.countNewsEvents(df, newsTypesConfig);
        log.info("========= COUNTING ENDED =========");

        groupedAndCounted.toDF(groupedAndCounted.columns())
                .write()
                .option("header", true)
                .csv(hdfsPrefix + outputDir);
    }
}
