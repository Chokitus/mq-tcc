package br.edu.ufabc.chokitus.benchmark

import br.edu.ufabc.chokitus.benchmark.data.TestResult
import br.edu.ufabc.chokitus.benchmark.data.TimedInterval
import br.edu.ufabc.chokitus.benchmark.impl.configuration.TestConfiguration
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.Extensions.forEachRun
import java.nio.file.Paths
import kotlin.io.path.bufferedWriter
import kotlin.io.path.createDirectories
import kotlin.io.path.createFile
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This class should define and execute all tests for this implementation.
 *
 * @constructor Create empty Abstract benchmark
 * @param T The benchmark result's extra data.
 * @param T The benchmark's configuration
 */
abstract class AbstractBenchmark {

	protected val log: Logger = LoggerFactory.getLogger(javaClass)

	protected var testStartTime: Long = 0

	companion object {
		private const val NANO_TO_SECOND = 1_000_000_000L
	}

	fun doBenchmark(configuration: TestConfiguration, clientFactory: ClientFactory): Any {
		val result = clientFactory.use { factory ->
			factory.start()

			log.info("Preparing test!")
			prepareTest(configuration, factory)

			log.info("Test prepared, executing...")
			val result = runCatching { doBenchmarkImpl(configuration, factory) }

			log.info("Test executed, aggregating data...")
			cleanUp(configuration, clientFactory)
			result
		}
		return aggregateData(result).also {
			log.info("Data aggregated!")
		}
	}

	/**
	 * Prepares this test. This should try to create all queues, topics and structures necessary to
	 * run the test. If there's no admin API for this message queue, all structures should have been
	 * created before the test, using external methods, such as shell scripts.
	 *
	 * @param configuration C
	 * @param clientFactory AbstractClientFactory<out AbstractReceiver<*, AbstractMessage, *>, *, *>
	 */
	protected abstract fun prepareTest(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	)

	protected abstract fun cleanUp(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	)

	/**
	 *  Define and execute fully this test, blocking the Main thread until this is finished.
	 *
	 *  This should be the only benchmarked method.
	 */
	protected abstract fun doBenchmarkImpl(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	): TestResult

	/**
	 * This should aggregate any data produced by the benchmark, being data exchanged, sent intervals,
	 * or any other relevant data. This is not benchmarked, so any aggregations that take longer than
	 * normal should execute here.
	 *
	 * @return T
	 */
	open fun aggregateData(result: Result<TestResult>) {
		val resultData = result.getOrThrow()

		resultData.produceIntervals.forEachIndexed { producerIndex, intervals ->
			classify(intervals, producerIndex)
		}

		resultData.latenciesAndReceiveIntervals.forEachIndexed { index, (latencies, receive) ->
			classify(latencies, index)
			classify(receive, index)
		}

		val produceByTimestamps = resultData.produceIntervals.flatten().sortedBy { it.timestamp }
		val (latencyTimestamps, receiveTimestamps) = resultData.flattenSortedLatencyAndReceive()

		val prefix = "test_results/${time()}"

		Paths.get(prefix).createDirectories()

		Paths.get("$prefix/producer.csv")
			.toAbsolutePath()
			.createFile()
			.also { log.info("Saving producer timestamps to $it") }
			.bufferedWriter().use { writer ->
				produceByTimestamps.forEachRun {
					writer.write("$timestamp;$classification;$interval\n")
				}
			}

		Paths.get("$prefix/receiver_latency.csv")
			.toAbsolutePath()
			.createFile()
			.also { log.info("Saving receiver latency timestamps to $it") }
			.bufferedWriter().use { writer ->
				latencyTimestamps.forEachRun {
					writer.write("$timestamp;$classification;$interval\n")
				}
			}

		Paths.get("$prefix/receiver.csv")
			.toAbsolutePath()
			.createFile()
			.also { log.info("Saving receiver timestamps to $it") }
			.bufferedWriter()
			.use { writer ->
				receiveTimestamps.forEachRun {
					writer.write("$timestamp;$classification;$interval\n")
				}
			}
	}

	private fun classify(
		list: List<TimedInterval>,
		index: Int
	) {
		list.forEach {
			it.classification = index
		}
	}

	protected fun time() = System.nanoTime()

	protected fun secondsFromStart(time: Long) =
		(time - testStartTime) / NANO_TO_SECOND

}

typealias ClientReceiver = AbstractReceiver<out Any, out AbstractMessage, out ClientProperties>
typealias ClientProducer = AbstractProducer<out Any, out AbstractMessage, out ClientProperties>
typealias ClientFactory =
		AbstractClientFactory<out ClientReceiver, out ClientProducer, out ClientProperties>
