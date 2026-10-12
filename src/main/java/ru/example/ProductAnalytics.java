package ru.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

/** Расчёт топа товаров по проданному количеству. */
public final class ProductAnalytics {
    private ProductAnalytics() {
    }

    public static List<Tuple2<String, ProductMetrics>> top5(JavaRDD<Transaction> sales) {
        JavaPairRDD<String, Tuple2<Long, BigDecimal>> totals = sales
                .mapToPair(row -> new Tuple2<>(
                        row.stockCode,
                        new Tuple2<>(row.quantity, row.lineTotal())
                ))
                .reduceByKey((left, right) -> new Tuple2<>(
                        left._1 + right._1,
                        left._2.add(right._2)
                ));

        JavaPairRDD<String, Long> orderCounts = sales
                .mapToPair(row -> new Tuple2<>(
                        row.stockCode + "\u0000" + row.invoiceNo, 1
                ))
                .keys()
                .distinct()
                .mapToPair(key -> new Tuple2<>(key.split("\u0000", 2)[0], 1L))
                .reduceByKey(Long::sum);

        JavaPairRDD<String, String> descriptions = sales
                .filter(row -> !row.description.isEmpty())
                .mapToPair(row -> new Tuple2<>(
                        row.stockCode + "\u0000" + row.description, 1L
                ))
                .reduceByKey(Long::sum)
                .mapToPair(pair -> {
                    String[] key = pair._1.split("\u0000", 2);
                    return new Tuple2<>(key[0], new DescriptionCount(key[1], pair._2));
                })
                .reduceByKey((left, right) -> DESCRIPTION_ORDER.compare(left, right) <= 0
                        ? left : right)
                .mapValues(value -> value.description);

        JavaRDD<Tuple2<String, ProductMetrics>> metrics = totals
                .join(orderCounts)
                .leftOuterJoin(descriptions)
                .map(pair -> {
                    Tuple2<Tuple2<Tuple2<Long, BigDecimal>, Long>,
                            org.apache.spark.api.java.Optional<String>> value = pair._2;
                    return new Tuple2<>(pair._1, new ProductMetrics(
                            value._2.orElse(""),
                            value._1._1._1,
                            value._1._2,
                            value._1._1._2
                    ));
                });

        return metrics.takeOrdered(5, new ProductOrder());
    }

    private static final Comparator<DescriptionCount> DESCRIPTION_ORDER =
            new DescriptionOrder();

    private static final class ProductOrder
            implements Comparator<Tuple2<String, ProductMetrics>>, Serializable {
        @Override
        public int compare(
                Tuple2<String, ProductMetrics> left,
                Tuple2<String, ProductMetrics> right) {
            int byQuantity = Long.compare(
                    right._2.totalQuantity, left._2.totalQuantity
            );
            return byQuantity != 0 ? byQuantity : left._1.compareTo(right._1);
        }
    }

    private static final class DescriptionOrder
            implements Comparator<DescriptionCount>, Serializable {
        @Override
        public int compare(DescriptionCount left, DescriptionCount right) {
            int byCount = Long.compare(right.count, left.count);
            return byCount != 0
                    ? byCount
                    : left.description.compareTo(right.description);
        }
    }

    private static final class DescriptionCount implements Serializable {
        final String description;
        final long count;

        DescriptionCount(String description, long count) {
            this.description = description;
            this.count = count;
        }
    }
}
