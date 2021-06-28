package br.edu.ufabc.chokitus.benchmark.impl.unicast

import br.edu.ufabc.chokitus.benchmark.AbstractBenchmark
import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.ClientProducer
import br.edu.ufabc.chokitus.benchmark.ClientReceiver
import br.edu.ufabc.chokitus.benchmark.impl.configuration.SingleDestinationConfiguration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.allOf
import java.util.concurrent.CompletableFuture.runAsync
import java.util.concurrent.CompletableFuture.supplyAsync
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.math3.stat.StatUtils

class SingleDestinationSingleOpsBenchmark : AbstractBenchmark<SingleDestinationConfiguration>() {

	private lateinit var testExecutor: ExecutorService

	private var testStartTime: Long = 0
	private var testEndTime: Long = 0

	companion object {
		private const val NANO_TO_SECOND = 1_000_000_000L
	}

	private val message: String = UUID.randomUUID().toString()
	private val queue: String = UUID.randomUUID().toString()

	private val messageSize = message.length
	private val activeProducers = AtomicInteger(0)

	private lateinit var latencies: List<Pair<Long, Long>>

	data class TestResult(
		val percentile1: Double,
		val percentile50: Double,
		val percentile99: Double,
		val max: Double,
		val average: Double
	)

	override fun aggregateData(result: Result<Unit>): Map<Long, TestResult> {
		result.getOrThrow()
		val grouped = latencies.groupBy(
			{ it.first },
			{ it.second / 10000 / 100.0 }
		)
		return grouped.mapValues { (key, value) ->
			val asArray = value.toDoubleArray()
			TestResult(
				percentile1 = StatUtils.percentile(asArray, 1.0),
				percentile50 = StatUtils.percentile(asArray, 50.0),
				percentile99 = StatUtils.percentile(asArray, 99.0),
				max = value.maxOf { it },
				average = value.average()
			)
		}

	}

	override fun doBenchmarkImpl(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		testStartTime = System.nanoTime()

		val receiver: List<CompletableFuture<List<Pair<Long, Long>>>> =
			(1..configuration.receiverCount).map {
				supplyAsync(
					{
						log.info("Creating receiver $it out of ${configuration.receiverCount}...")
						doReceiver(clientFactory.createReceiver(), configuration)
					},
					testExecutor
				)
			}

		val producers: List<CompletableFuture<Void>> = (1..configuration.producerCount).map {
			runAsync(
				{
					log.info("Creating producer $it out of ${configuration.producerCount}...")
					doProducer(clientFactory.createProducer(), configuration)
				},
				testExecutor
			)
		}

		allOf(*producers.toTypedArray()).get()

		this.latencies = receiver.flatMap { it.get() }
	}

	override fun prepareTest(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		with(configuration) {
			testExecutor = Executors.newFixedThreadPool(producerCount + receiverCount)
			activeProducers.set(configuration.producerCount)

			clientFactory.createQueue(queue)
		}
	}

	override fun cleanUp(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		runCatching {
			clientFactory.deleteQueue(queue)

			testExecutor.shutdown()
			testExecutor.awaitTermination(60, TimeUnit.SECONDS)
		}
	}

	private fun doReceiver(
		receiver: ClientReceiver,
		configuration: SingleDestinationConfiguration
	): List<Pair<Long, Long>> {
		val latencies = ArrayList<Pair<Long, Long>>()
		fun observe(receivedTime: Long, sentTime: Long) =
			latencies.add(
				(receivedTime - testStartTime) / NANO_TO_SECOND to receivedTime - sentTime
			)

		log.info("Starting receiver...")
		receiver.start()
		log.info("Receiver started successfully! Will now proceed to test...")
		while (activeProducers.get() > 0) {
			receiver.receive(queue)?.let { message ->
				val receivedTime = System.nanoTime()
				val sentTime = message.body().decodeToString().substring(messageSize).toLong()

				observe(receivedTime, sentTime)
				message.ack()
			}
		}
		log.info("All producers finished their work, stopping...")
		return latencies
	}

	private fun doProducer(
		producer: ClientProducer,
		configuration: SingleDestinationConfiguration
	) {
		log.info("Starting receiver...")
		producer.start()
		log.info("Producer started successfully! Will now proceed to test...")
		repeat(configuration.messageCount) {
			producer.produce(queue, "$message${System.nanoTime()}".encodeToByteArray())
		}
		activeProducers.decrementAndGet()
	}

}
