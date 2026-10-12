package ru.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

/** Точка входа: загрузка, проверки, расчёты и публикация витрин. */
public final class RetailApp {
    private RetailApp() {
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Использование: RetailApp <input.csv> [output-directory]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputDirectory = args.length == 2 ? args[1] : "output";
        SparkConf configuration = new SparkConf().setAppName("Online Retail Lab 2");

        try (JavaSparkContext context = new JavaSparkContext(configuration)) {
            context.setLogLevel("WARN");
            DatasetLoader.LoadResult loaded = DatasetLoader.load(context, inputPath);
            DatasetValidator.assertNoCustomerConflicts(loaded.clean);

            JavaRDD<Transaction> sales = loaded.clean
                    .filter(DatasetValidator::isSale)
                    .persist(StorageLevel.DISK_ONLY());
            long salesCount = sales.count();
            long creditCount = loaded.clean.filter(DatasetValidator::isCredit).count();
            long rejectedCount = loaded.cleanCount - salesCount - creditCount;

            System.out.println("Исходных строк: " + loaded.sourceCount);
            System.out.println("После дедупликации: " + loaded.cleanCount);
            System.out.println("Удалено дублей: " + (loaded.sourceCount - loaded.cleanCount));
            System.out.println("Строк продаж: " + salesCount);
            System.out.println("Строк возвратов: " + creditCount);
            System.out.println("Отклонённых строк: " + rejectedCount);

            JavaRDD<Transaction> productSales = sales
                    .filter(DatasetValidator::isProduct);
            List<Tuple2<String, ProductMetrics>> top5 = ProductAnalytics.top5(productSales);

            System.out.println("Топ-5 товаров:");
            for (Tuple2<String, ProductMetrics> item : top5) {
                ProductMetrics metric = item._2;
                System.out.printf("%s | %d | %d | %s%n",
                        item._1,
                        metric.totalQuantity,
                        metric.ordersWithProduct,
                        ResultWriter.money(metric.grossValue));
            }

            CustomerAnalytics.Result customerResult =
                    CustomerAnalytics.calculate(loaded.clean, sales);
            long ordersCount = customerResult.orders.count();
            long customersCount = customerResult.customerMetrics.count();

            ResultWriter.writeTop5(context, top5, outputDirectory);
            ResultWriter.writeOrders(context, customerResult.orders, outputDirectory);
            ResultWriter.writeCustomerMetrics(
                    context, customerResult.customerMetrics, outputDirectory
            );

            System.out.println("Заказов известных клиентов: " + ordersCount);
            System.out.println("Известных клиентов: " + customersCount);
            System.out.println("Результаты сохранены в: " + outputDirectory);

            customerResult.customerMetrics.unpersist();
            customerResult.orders.unpersist();
            sales.unpersist();
            loaded.clean.unpersist();
        }
    }
}
