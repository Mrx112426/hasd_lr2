package ru.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/** Расчёт топа товаров по проданному количеству. */
public final class ProductAnalytics {
    private ProductAnalytics() {
    }

    public static List<Tuple2<String, ProductMetrics>> top5(JavaRDD<Transaction> sales) {
        JavaPairRDD<String, ProductMetrics> totals = sales
                .mapToPair(row -> new Tuple2<>(
                        row.stockCode,
                        new ProductMetrics("", row.quantity, 0, row.lineTotal())
                ))
                .reduceByKey(ProductMetrics::add);

        JavaPairRDD<String, ProductMetrics> withOrders = totals
                .join(countOrders(sales))
                .mapValues(pair -> pair._1.withOrderCount(pair._2));

        JavaPairRDD<String, ProductMetrics> withNames = withOrders
                .leftOuterJoin(chooseDescriptions(sales))
                .mapValues(pair -> pair._1.withDescription(pair._2.orElse("")));

        return withNames.takeOrdered(5, new ProductOrder());
    }

    private static JavaPairRDD<String, Long> countOrders(JavaRDD<Transaction> sales) {
        return sales
                .map(row -> new Tuple2<>(row.stockCode, row.invoiceNo))
                .distinct()
                .mapToPair(pair -> new Tuple2<>(pair._1, 1L))
                .reduceByKey(Long::sum);
    }

    private static JavaPairRDD<String, String> chooseDescriptions(JavaRDD<Transaction> sales) {
        // Самое частое непустое название; при равенстве — первое по алфавиту.
        return sales
                .filter(row -> !row.description.isEmpty())
                .mapToPair(row -> new Tuple2<>(
                        new Tuple2<>(row.stockCode, row.description), 1L
                ))
                .reduceByKey(Long::sum)
                .mapToPair(pair -> new Tuple2<>(pair._1._1,
                        new DescriptionCount(pair._1._2, pair._2)))
                .reduceByKey(DescriptionCount::choose)
                .mapValues(value -> value.description);
    }

    // Компаратор передаётся исполнителю Spark, поэтому нужен Serializable.
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

    private static final class DescriptionCount implements Serializable {
        final String description;
        final long count;

        DescriptionCount(String description, long count) {
            this.description = description;
            this.count = count;
        }

        DescriptionCount choose(DescriptionCount other) {
            if (count != other.count) {
                return count > other.count ? this : other;
            }
            return description.compareTo(other.description) <= 0 ? this : other;
        }
    }
}
