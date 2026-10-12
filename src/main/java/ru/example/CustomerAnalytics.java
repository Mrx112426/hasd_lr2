package ru.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;

/** Построение витрины заказов и клиентских метрик. */
public final class CustomerAnalytics {
    private CustomerAnalytics() {
    }

    public static Result calculate(
            JavaRDD<Transaction> cleanRows,
            JavaRDD<Transaction> sales) {
        JavaPairRDD<String, BigDecimal> orders = sales
                .filter(row -> !row.customerId.isEmpty())
                .mapToPair(row -> new Tuple2<>(
                        row.customerId + "\u0000" + row.invoiceNo,
                        row.lineTotal()
                ))
                .reduceByKey(BigDecimal::add)
                .persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String, Tuple2<Long, BigDecimal>> calculated = orders
                .mapToPair(pair -> new Tuple2<>(
                        pair._1.split("\u0000", 2)[0],
                        new Tuple2<>(1L, pair._2)
                ))
                .reduceByKey((left, right) -> new Tuple2<>(
                        left._1 + right._1,
                        left._2.add(right._2)
                ));

        JavaPairRDD<String, Integer> allCustomers = cleanRows
                .filter(row -> !row.customerId.isEmpty())
                .mapToPair(row -> new Tuple2<>(row.customerId, 1))
                .reduceByKey((left, right) -> 1);

        JavaPairRDD<String, CustomerMetrics> customerMetrics = allCustomers
                .leftOuterJoin(calculated)
                .mapValues((Tuple2<Integer, Optional<Tuple2<Long, BigDecimal>>> value) -> {
                    if (value._2.isPresent()) {
                        Tuple2<Long, BigDecimal> metric = value._2.get();
                        return new CustomerMetrics(metric._1, metric._2);
                    }
                    return new CustomerMetrics(0, BigDecimal.ZERO);
                })
                .persist(StorageLevel.MEMORY_AND_DISK());

        return new Result(orders, customerMetrics);
    }

    public static final class Result implements Serializable {
        final JavaPairRDD<String, BigDecimal> orders;
        final JavaPairRDD<String, CustomerMetrics> customerMetrics;

        Result(
                JavaPairRDD<String, BigDecimal> orders,
                JavaPairRDD<String, CustomerMetrics> customerMetrics) {
            this.orders = orders;
            this.customerMetrics = customerMetrics;
        }
    }
}
