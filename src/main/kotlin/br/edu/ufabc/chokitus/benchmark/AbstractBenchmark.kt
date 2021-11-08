package br.edu.ufabc.chokitus.benchmark

import br.edu.ufabc.chokitus.benchmark.data.FinalResult
import br.edu.ufabc.chokitus.benchmark.data.TestResult
import br.edu.ufabc.chokitus.benchmark.data.TimedInterval
import br.edu.ufabc.chokitus.benchmark.impl.configuration.TestConfiguration
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.ArgumentParser
import br.edu.ufabc.chokitus.util.Extensions.forEachRun
import java.io.BufferedWriter
import java.nio.file.Paths
import kotlin.io.path.bufferedWriter
import kotlin.io.path.createDirectories
import kotlin.io.path.createFile
import kotlin.math.abs
import kotlin.math.sqrt
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This class should define and execute all tests for this implementation.
 *
 * @constructor Create empty Abstract benchmark
 */
abstract class AbstractBenchmark(
	protected val arguments: ArgumentParser.ParseResult,
	val messageSize: Int,
	val messageCount: Int
) {

	private val configs: String = arguments.toString()

	protected val log: Logger = LoggerFactory.getLogger(javaClass)

	protected var testStartTime: Long = 0

	companion object {
		private const val NANO_TO_SECOND = 1_000_000_000L
		private const val NANO_TO_MILLI = 1_000_000.00
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

		val produce = resultData.produceIntervals
		val (latency, receive) = resultData.latenciesAndReceiveIntervals
			.fold(
				listOf<TimedInterval>() to listOf<TimedInterval>()
			) { (accLat, accRec), (lat, rec) ->
				(accLat + lat) to (accRec + rec)
			}

		val producerResults = extractResults(produce.flatten(), arguments.producers)
		val latencyResults = extractResults(latency, arguments.consumers)
		val receiveResults = extractResults(receive, arguments.consumers)

		val prefix = "test_results/$configs/${time()}"

		Paths.get(prefix).createDirectories()

		Paths.get("$prefix/producer.csv")
			.toAbsolutePath()
			.createFile()
			.also { log.info("Saving producer timestamps to $it") }
			.bufferedWriter().use { writer ->
				writeValues(writer, producerResults)
			}

		Paths.get("$prefix/receiver_latency.csv")
			.toAbsolutePath()
			.createFile()
			.also { log.info("Saving receiver latency timestamps to $it") }
			.bufferedWriter().use { writer ->
				writeValues(writer, latencyResults)
			}

		Paths.get("$prefix/receiver.csv")
			.toAbsolutePath()
			.createFile()
			.also { log.info("Saving receiver timestamps to $it") }
			.bufferedWriter()
			.use { writer ->
				writeValues(writer, receiveResults)
			}
	}

	private fun writeValues(
		writer: BufferedWriter,
		receiveResults: List<FinalResult>
	) {
		val client = arguments.client
		val producer = arguments.producers
		val consumer = arguments.consumers
		val messageSize = arguments.messageSize
		val benchmark = arguments.benchmark
		val destinations = arguments.destinations

		val maxTimestamp = receiveResults.maxOf { it.timestamp }.toDouble()

		writer.write(
			"timestamp;" +
					"normal_timestamp;" +
					"client;" +
					"producers;" +
					"consumers;" +
					"message_size;" +
					"benchmark;" +
					"destinations;" +
					"max_latency;" +
					"min_latency;" +
					"avg_latency;" +
					"latency_std;" +
					"balance;" +
					"quantity"
		)
		writer.newLine()
		receiveResults.forEachRun {
			writer.write(
				(
						"" +
								"$timestamp;" +
								"${timestamp / maxTimestamp};" +
								"$client;" +
								"$producer;" +
								"$consumer;" +
								"$messageSize;" +
								"$benchmark;" +
								"$destinations;" +
								"$max;" +
								"$min;" +
								"$avg;" +
								"$std;" +
								"$load;" +
								"$count"
						).replace('.', ',')
			)
			writer.newLine()
		}
	}

	private fun extractResults(values: List<TimedInterval>, actors: Int) =
		values
			.groupBy { it.timestamp }
			.map { (timestamp, entries) ->
				val latencies = entries.map { (it.interval.toDouble() / NANO_TO_MILLI).precision() }

				/**
				 * How many each consumer processed
				 */
				val count = entries
					.groupBy { it.classification }
					.mapValues { it.value.size }

				/**
				 * Max value should be
				 */
				val maxValue = ((actors - 1.0) * 2.0 / actors).takeIf { it > 0 } ?: 1.0
				val average = latencies.size.toDouble() / actors

				val sumOfValues = count.values.sum()
				val sumOfDifferences = count.mapValues { abs(average - it.value) }.values.sum()

				val loadFactor = sumOfDifferences / sumOfValues.toDouble()

				FinalResult(
					timestamp = timestamp,
					avg = latencies.average().precision(),
					max = latencies.maxOrNull()!!,
					min = latencies.minOrNull()!!,
					std = latencies.std().precision(),
					load = (1.0 - loadFactor / maxValue),
					count = latencies.size,
				)
			}

	protected fun time() = System.nanoTime()

	protected fun timestamp(time: Long): Long =
		(time - testStartTime) / NANO_TO_SECOND / 5

	private fun Double.precision(): Double =
		(this * 100).toInt() / 100.0

	private fun List<Double>.std(): Double =
		average().let { avg ->
			sqrt(sumOf { (it - avg) * (it - avg) } / size)
		}

}

typealias ClientReceiver = AbstractReceiver<out Any, out AbstractMessage, out ClientProperties>
typealias ClientProducer = AbstractProducer<out Any, out AbstractMessage, out ClientProperties>
typealias ClientFactory =
		AbstractClientFactory<out ClientReceiver, out ClientProducer, out ClientProperties>
