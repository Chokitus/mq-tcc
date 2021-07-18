package br.edu.ufabc.chokitus.benchmark.impl.unicast

import br.edu.ufabc.chokitus.benchmark.AbstractBenchmark
import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.ClientProducer
import br.edu.ufabc.chokitus.benchmark.ClientReceiver
import br.edu.ufabc.chokitus.benchmark.data.TestResult
import br.edu.ufabc.chokitus.benchmark.data.TimedInterval
import br.edu.ufabc.chokitus.benchmark.data.timing
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ProducerConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ReceiverConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.SingleDestinationConfiguration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.supplyAsync
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.lang3.RandomStringUtils

class SingleDestinationSingleOpsBenchmark : AbstractBenchmark<SingleDestinationConfiguration>() {

	private lateinit var testExecutor: ExecutorService

	private lateinit var defaultMessage: String

	private val activeProducers = AtomicInteger(0)

	override fun doBenchmarkImpl(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	): TestResult {
		testStartTime = time()
		defaultMessage = RandomStringUtils.random(configuration.messageSize)

		val receiver: List<CompletableFuture<Pair<List<TimedInterval>, List<TimedInterval>>>> =
			(1..configuration.receiverCount).map {
				supplyAsync(
					{
						log.info("Creating receiver $it out of ${configuration.receiverCount}...")
						doReceiver(clientFactory.createReceiver(), configuration.receiverConfigurations[it])
					},
					testExecutor
				)
			}

		val producers: List<CompletableFuture<List<TimedInterval>>> =
			(1..configuration.producerCount).map {
				supplyAsync(
					{
						log.info("Creating producer $it out of ${configuration.producerCount}...")
						doProducer(
							producer = clientFactory.createProducer(),
							messageCount = configuration.messageCount,
							configuration = configuration.producerConfigurations[it]
						)
					},
					testExecutor
				)
			}

		return TestResult(
			latenciesAndReceiveIntervals = receiver.map { it.get() },
			produceIntervals = producers.map { it.get() }
		)
	}

	override fun prepareTest(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		with(configuration) {
			testExecutor = Executors.newFixedThreadPool(producerCount + receiverCount)
			activeProducers.set(configuration.producerCount)

		}
	}

	override fun cleanUp(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		runCatching {
			clientFactory.cleanUpDestinations()

			testExecutor.shutdown()
			testExecutor.awaitTermination(60, TimeUnit.SECONDS)
		}
	}

	private fun doReceiver(
		receiver: ClientReceiver,
		configuration: ReceiverConfiguration
	): Pair<ArrayList<TimedInterval>, ArrayList<TimedInterval>> {
		val latenciesWithTimestamp = ArrayList<TimedInterval>()
		val intervalWithTimestamp = ArrayList<TimedInterval>()

		fun observe(receivedTime: Long, sentTime: Long, requestTime: Long) {
			val timestamp = secondsFromStart(receivedTime)
			val latency = receivedTime - sentTime
			val receiveInterval = receivedTime - requestTime
			latenciesWithTimestamp.add(timestamp timing latency)
			intervalWithTimestamp.add(timestamp timing receiveInterval)
		}

		log.info("Starting receiver...")
		receiver.start()
		log.info("Receiver started successfully! Will now proceed to test...")
		var receivedAny = false
		while (activeProducers.get() > 0 || receivedAny) {
			val requestTime = time()
			val message = receiver.receive(configuration.queueName, configuration)
			val receivedTime = time()
			receivedAny = message != null
			if (message != null) {
				val sentTime = message.body().decodeToString().substring(defaultMessage.length).toLong()

				observe(receivedTime, sentTime, requestTime)
				message.ack()
			}
		}
		log.info("All producers finished their work, stopping...")
		return latenciesWithTimestamp to intervalWithTimestamp
	}

	private fun doProducer(
		producer: ClientProducer,
		messageCount: Int,
		configuration: ProducerConfiguration,
	): ArrayList<TimedInterval> {
		val requestIntervalsWithTimestamp = ArrayList<TimedInterval>()
		fun observe(requestTime: Long, producedTime: Long) {
			val timestamp = secondsFromStart(requestTime)
			val interval = producedTime - requestTime
			requestIntervalsWithTimestamp.add(timestamp timing interval)
		}

		log.info("Starting receiver...")
		producer.start()
		log.info("Producer started successfully! Will now proceed to test...")
		repeat(messageCount) {
			val requestTime = time()
			producer.produce(configuration.destinationName, "$defaultMessage${time()}".encodeToByteArray())
			val producedTime = time()
			observe(requestTime, producedTime)
		}
		activeProducers.decrementAndGet()

		return requestIntervalsWithTimestamp
	}

}
