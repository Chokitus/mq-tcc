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
import br.edu.ufabc.chokitus.benchmark.impl.configuration.TestConfiguration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.supplyAsync
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.lang3.RandomStringUtils

class SingleOpsBenchmark : AbstractBenchmark() {

	private lateinit var testExecutor: ExecutorService

	private lateinit var defaultMessage: String

	private val activeProducers = AtomicInteger(0)

	override fun doBenchmarkImpl(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	): TestResult {
		testStartTime = time()
		defaultMessage = RandomStringUtils.random(configuration.messageSize)

		val receiver: List<CompletableFuture<Pair<List<TimedInterval>, List<TimedInterval>>>> =
			(0 until configuration.receiverCount).map {
				supplyAsync(
					{
						log.info("Creating receiver $it out of ${configuration.receiverCount}...")
						doReceiver(
							receiver = clientFactory.createReceiver(),
							receiverConfiguration = configuration.receiverConfigurations[it],
							receiverId = it
						)
					},
					testExecutor
				)
			}

		val producers: List<CompletableFuture<List<TimedInterval>>> =
			(0 until configuration.producerCount).map {
				supplyAsync(
					{
						log.info("Creating producer $it out of ${configuration.producerCount}...")
						doProducer(
							producer = clientFactory.createProducer(),
							messageCount = configuration.messageCount,
							producerConfiguration = configuration.producerConfigurations[it],
							producerId = it
						)
					},
					testExecutor
				)
			}

		return TestResult(
			produceIntervals = producers.map { it.get() },
			latenciesAndReceiveIntervals = receiver.map { it.get() }
		)
	}

	override fun prepareTest(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	) {
		with(configuration) {
			val threadCount = producerCount + receiverCount
			log.info("Initializing $threadCount threads...")
			testExecutor = Executors.newFixedThreadPool(producerCount + receiverCount)

			log.info("Setting active producers to $producerCount...")
			activeProducers.set(producerCount)

			log.info("Creating ${destinationConfigurations.size} destinations...")
			destinationConfigurations
				.forEach(clientFactory::createDestination)

		}
	}

	override fun cleanUp(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	) {
		runCatching {
			testExecutor.shutdown()
			testExecutor.awaitTermination(60, TimeUnit.SECONDS)

			clientFactory.cleanUpDestinations()
		}
			.getOrThrow()
	}

	private fun doReceiver(
		receiver: ClientReceiver,
		receiverConfiguration: ReceiverConfiguration,
		receiverId: Int
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

		log.info("$receiverId: Starting receiver...")
		receiver.use {
			receiver.start()
			log.info("$receiverId: Receiver started successfully! Will now proceed to test...")
			var receivedAny = false
			while (activeProducers.get() > 0 || receivedAny) {
				val requestTime = time()
				val message = receiver.receive(receiverConfiguration.queueName, receiverConfiguration)
				val receivedTime = time()
				receivedAny = message != null
				if (message != null) {
					if (latenciesWithTimestamp.size % 50 == 0) {
						log.info("$receiverId: Received ${latenciesWithTimestamp.size} messages...")
					}
					val sentTime = message.bodyAsString().substring(defaultMessage.length).toLong()

					observe(receivedTime, sentTime, requestTime)
					message.ack()
				}
			}
			log.info("$receiverId: All producers finished their work, stopping...")
		}
		return latenciesWithTimestamp to intervalWithTimestamp
	}

	private fun doProducer(
		producer: ClientProducer,
		messageCount: Int,
		producerConfiguration: ProducerConfiguration,
		producerId: Int
	): ArrayList<TimedInterval> {
		val requestIntervalsWithTimestamp = ArrayList<TimedInterval>()
		fun observe(requestTime: Long, producedTime: Long) {
			val timestamp = secondsFromStart(requestTime)
			val interval = producedTime - requestTime
			requestIntervalsWithTimestamp.add(timestamp timing interval)
		}

		log.info("$producerId: Starting receiver...")
		producer.use {
			producer.start()
			log.info("$producerId: Producer started successfully! Will now proceed to test...")
			repeat(messageCount) {
				if (it % 50 == 0) {
					log.info("$producerId: Produced $it messages...")
				}
				val requestTime = time()
				producer.produce(
					producerConfiguration.destinationName,
					"$defaultMessage${time()}".encodeToByteArray()
				)
				val producedTime = time()
				observe(requestTime, producedTime)
			}
			activeProducers.decrementAndGet()
		}
		log.info("$producerId: Producer finished successfully!")

		return requestIntervalsWithTimestamp
	}

}
