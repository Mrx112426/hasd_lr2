package ru.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/** Запись итоговых CSV-витрин. */
public final class ResultWriter {
    private ResultWriter() {
    }

    public static void writeTop5(
            JavaSparkContext context,
            List<Tuple2<String, ProductMetrics>> top,
            String outputDirectory) {
        List<String> lines = new ArrayList<>();
        lines.add("StockCode,Description,total_quantity,orders_with_product,gross_value_gbp");
        for (Tuple2<String, ProductMetrics> item : top) {
            ProductMetrics metric = item._2;
            lines.add(csv(item._1) + "," + csv(metric.description)
                    + "," + metric.totalQuantity
                    + "," + metric.ordersWithProduct
                    + "," + money(metric.grossValue));
        }
        context.parallelize(lines, 1).saveAsTextFile(outputDirectory + "/top5_products.csv");
    }

    public static void writeOrders(
            JavaSparkContext context,
            JavaPairRDD<String, BigDecimal> orders,
            String outputDirectory) {
        JavaRDD<String> rows = orders.sortByKey(true)
                .map(pair -> {
                    String[] key = pair._1.split("\u0000", 2);
                    return csv(key[0]) + "," + csv(key[1])
                            + "," + decimal4(pair._2);
                });

        saveWithHeader(
                context,
                "CustomerID,InvoiceNo,order_total",
                rows,
                outputDirectory + "/orders.csv"
        );
    }

    public static void writeCustomerMetrics(
            JavaSparkContext context,
            JavaPairRDD<String, CustomerMetrics> metrics,
            String outputDirectory) {
        JavaRDD<String> rows = metrics.sortByKey(true)
                .map(pair -> {
                    CustomerMetrics metric = pair._2;
                    BigDecimal average = metric.averageOrderValue();
                    return csv(pair._1) + "," + metric.ordersCount
                            + "," + money(metric.totalSpent)
                            + "," + (average == null ? "" : money(average));
                });

        saveWithHeader(
                context,
                "CustomerID,orders_count,total_spent,average_order_value",
                rows,
                outputDirectory + "/customer_metrics.csv"
        );
    }

    private static void saveWithHeader(
            JavaSparkContext context,
            String header,
            JavaRDD<String> rows,
            String path) {
        context.parallelize(List.of(header), 1)
                .union(rows.coalesce(1))
                .coalesce(1)
                .saveAsTextFile(path);
    }

    static String money(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    private static String decimal4(BigDecimal value) {
        return value.setScale(4, RoundingMode.HALF_UP).toPlainString();
    }

    private static String csv(String value) {
        if (value.contains(",") || value.contains("\"")
                || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
