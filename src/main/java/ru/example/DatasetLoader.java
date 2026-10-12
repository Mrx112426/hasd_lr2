package ru.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.nio.charset.Charset;
import java.util.List;

/** Загрузка Windows-1252 CSV и дедупликация восьми исходных полей. */
public final class DatasetLoader {
    private DatasetLoader() {
    }

    public static LoadResult load(JavaSparkContext context, String path) {
        JavaRDD<List<String>> raw = context
                .binaryFiles(path, 1)
                .values()
                .flatMap(file -> {
                    String text = new String(
                            file.toArray(), Charset.forName("windows-1252")
                    );
                    return CsvParser.parse(text).iterator();
                })
                .filter(row -> !row.isEmpty() && !"InvoiceNo".equals(row.get(0)))
                .persist(StorageLevel.DISK_ONLY());

        long sourceCount = raw.count();
        JavaRDD<Transaction> clean = raw
                .distinct()
                .map(Transaction::fromRow)
                .persist(StorageLevel.DISK_ONLY());
        long cleanCount = clean.count();
        raw.unpersist();

        return new LoadResult(sourceCount, cleanCount, clean);
    }

    public static final class LoadResult {
        final long sourceCount;
        final long cleanCount;
        final JavaRDD<Transaction> clean;

        LoadResult(long sourceCount, long cleanCount, JavaRDD<Transaction> clean) {
            this.sourceCount = sourceCount;
            this.cleanCount = cleanCount;
            this.clean = clean;
        }
    }
}
