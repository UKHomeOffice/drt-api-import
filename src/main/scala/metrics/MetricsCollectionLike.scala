package metrics

import github.gphat.censorinus.StatsDClient
import org.slf4j.{Logger, LoggerFactory}

trait MetricsCollectorLike {
  def counter(name: String, value: Double): Unit
}

object StatsDMetrics extends MetricsCollectorLike {
  val statsd: StatsDClient = new StatsDClient()

  override def counter(name: String, value: Double): Unit = statsd.counter(name, value)
}

object LoggingMetrics extends MetricsCollectorLike {
  val log: Logger = LoggerFactory.getLogger(getClass)

  override def counter(name: String, value: Double): Unit = log.info(s"$name count: $value")
}
