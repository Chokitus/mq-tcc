package br.edu.ufabc.chokitus.benchmark

import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This class should define and execute all tests for this implementation.
 *
 * @constructor Create empty Abstract benchmark
 * @param T The benchmark result's extra data.
 * @param T The benchmark's configuration
 */
abstract class AbstractBenchmark<C, T> {

	protected val log: Logger = LoggerFactory.getLogger(javaClass)

	fun doBenchmark(configuration: C, clientFactory: ClientFactory): T? {
		log.info("Preparing test!")
		prepareTest(configuration, clientFactory)
		log.info("Test prepared, executing...")
		val result = runCatching { clientFactory.use { doBenchmarkImpl(configuration, it) } }
		log.info("Test executed, aggregating data...")
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
		configuration: C,
		clientFactory: ClientFactory
	)

	protected abstract fun cleanUp(configuration: C, clientFactory: ClientFactory): Unit

	/**
	 *  Define and execute fully this test, blocking the Main thread until this is finished.
	 *
	 *  This should be the only benchmarked method.
	 */
	protected abstract fun doBenchmarkImpl(
		configuration: C,
		clientFactory: ClientFactory
	)

	/**
	 * This should aggregate any data produced by the benchmark, being data exchanged, sent intervals,
	 * or any other relevant data. This is not benchmarked, so any aggregations that take longer than
	 * normal should execute here.
	 *
	 * @return T
	 */
	open fun aggregateData(result: Result<Unit>): T? {
		result.getOrThrow()
		return null
	}

}

typealias ClientFactory = AbstractClientFactory<out AbstractReceiver<*, AbstractMessage, *>, *, *>
