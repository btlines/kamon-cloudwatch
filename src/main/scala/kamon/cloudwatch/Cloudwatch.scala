package kamon.cloudwatch

import java.time.Instant

import scala.collection.JavaConverters._
import com.typesafe.config.Config
import kamon.{Kamon, MetricReporter, Tags}
import kamon.metric.{PeriodSnapshot, MeasurementUnit => KamonUnit, MetricDistribution => KamonDistrib, MetricValue => KamonMetric}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.model.{PutMetricDataRequest, StatisticSet, Dimension => AwsTag, MetricDatum => AwsMetric, StandardUnit => AwsUnit}

class Cloudwatch(client: CloudWatchAsyncClient) extends MetricReporter {
  import Cloudwatch._

  private val logger = LoggerFactory.getLogger(classOf[Cloudwatch])

  override def start(): Unit =
    logger.info("Started the Cloudwatch reporter.")

  override def stop(): Unit =
    logger.info("Stopped the Cloudwatch reporter.")

  override def reconfigure(config: Config): Unit = ()

  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit =
    awsRequests(snapshot).foreach(client.putMetricData)
}

object Cloudwatch {
  def apply(): Cloudwatch =
    new Cloudwatch(CloudWatchAsyncClient.create())

  def apply(client: CloudWatchAsyncClient): Cloudwatch =
    new Cloudwatch(client)

  private val NamespaceConfigKey = "kamon.cloudwatch.namespace"
  private val MaxAwsMetrics = 20
  private val MaxAwsTags = 10

  private[cloudwatch] def awsTags(tags: Tags): Seq[AwsTag] =
    tags.take(MaxAwsTags).map {
      case (k, v) => AwsTag.builder().name(k).value(v).build()
    }.toSeq

  private[cloudwatch] def awsUnit(unit: KamonUnit): AwsUnit = {
    import KamonUnit._
    unit match {
      case none => AwsUnit.NONE
      case percentage => AwsUnit.PERCENT
      case information.bytes => AwsUnit.BYTES
      case information.kilobytes => AwsUnit.KILOBYTES
      case information.megabytes => AwsUnit.MEGABYTES
      case information.gigabytes => AwsUnit.GIGABYTES
      case time.seconds => AwsUnit.SECONDS
      case time.milliseconds => AwsUnit.MILLISECONDS
      case time.microseconds => AwsUnit.MICROSECONDS
      case time.nanoseconds => AwsUnit.MICROSECONDS // nanoseconds are not supported by Cloudwatch
      // unknown unit
      case KamonUnit(_, Magnitude(name, _)) => AwsUnit.fromValue(name)
    }
  }

  private[cloudwatch] def scaleValue(value: Long, unit: KamonUnit): Double = {
    import KamonUnit._
    unit match {
      case time.nanoseconds => scale(value, time.nanoseconds, time.microseconds)
      case _ => value
    }
  }

  private[cloudwatch] def awsMetric(timestamp: Instant, m: KamonMetric): AwsMetric =
    AwsMetric.builder()
      .metricName(m.name)
      .value(scaleValue(m.value, m.unit))
      .unit(awsUnit(m.unit))
      .timestamp(timestamp)
      .dimensions(awsTags(m.tags).asJava)
      .build()

  private[cloudwatch] def awsMetric(timestamp: Instant, m: KamonDistrib): AwsMetric =
    AwsMetric.builder()
      .metricName(m.name)
      .statisticValues(
        StatisticSet.builder()
          .maximum(scaleValue(m.distribution.max, m.unit))
          .minimum(scaleValue(m.distribution.min, m.unit))
          .sum(scaleValue(m.distribution.sum, m.unit))
          .sampleCount(m.distribution.count.toDouble)
          .build()
      )
      .storageResolution(m.dynamicRange.significantValueDigits)
      .unit(awsUnit(m.unit))
      .timestamp(timestamp)
      .dimensions(awsTags(m.tags).asJava)
      .build()

  private[cloudwatch] def awsRequests(snapshot: PeriodSnapshot): Seq[PutMetricDataRequest] = {
    val namespace = Kamon.config().getString(NamespaceConfigKey)
    val metrics = (snapshot.metrics.counters ++ snapshot.metrics.gauges).map(awsMetric(snapshot.from, _))
    val distribs = (snapshot.metrics.histograms ++ snapshot.metrics.rangeSamplers).map(awsMetric(snapshot.from, _))
    (metrics ++ distribs).grouped(MaxAwsMetrics).map(metrics =>
      PutMetricDataRequest.builder()
        .metricData(metrics.asJava)
        .namespace(namespace)
        .build()
    ).toSeq
  }
}

