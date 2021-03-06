package br.edu.ufabc.chokitus.benchmark.impl.unicast

import br.edu.ufabc.chokitus.benchmark.AbstractBenchmark
import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.ClientProducer
import br.edu.ufabc.chokitus.benchmark.ClientReceiver
import br.edu.ufabc.chokitus.benchmark.data.TestResult
import br.edu.ufabc.chokitus.benchmark.data.TimedInterval
import br.edu.ufabc.chokitus.benchmark.data.timingFor
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ProducerConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ReceiverConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.TestConfiguration
import br.edu.ufabc.chokitus.util.ArgumentParser
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.supplyAsync
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.lang3.RandomStringUtils

class SingleOpsBenchmark(
	arguments: ArgumentParser.ParseResult,
	messageSize: Int,
	messageCount: Int
) : AbstractBenchmark(
	arguments = arguments,
	messageSize = messageSize,
	messageCount = messageCount,
) {

	private lateinit var testExecutor: ExecutorService

	private lateinit var defaultMessage: String

	private val activeProducers = AtomicInteger(0)
	private val inactiveReceivers = AtomicInteger(0)

	fun activateReceiver() =
		inactiveReceivers.decrementAndGet()

	fun awaitForReceivers() {
		while (inactiveReceivers.get() != 0) Thread.sleep(5)
		println("Releasing from lock!")
	}

	private val printCount = 2000

	override fun doBenchmarkImpl(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	): TestResult {
		testStartTime = time()
		defaultMessage = RandomStringUtils.randomAlphanumeric(messageSize)

		val receiver: List<CompletableFuture<Pair<List<TimedInterval>, List<TimedInterval>>>> =
			(0 until configuration.receiverCount).map {
				supplyAsync(
					{
						log.info("Creating receiver $it out of ${configuration.receiverCount}...")
						val receiverConfiguration = configuration.receiverConfigurations[it]
						doReceiver(
							receiver = clientFactory.createReceiver(receiverConfiguration),
							receiverConfiguration = receiverConfiguration,
							receiverId = it
						)
					},
					testExecutor
				)
			}

		Thread.sleep(1000)

		val producers: List<CompletableFuture<List<TimedInterval>>> =
			(0 until configuration.producerCount).map {
				supplyAsync(
					{
						log.info("Creating producer $it out of ${configuration.producerCount}...")
						val producerConfiguration = configuration.producerConfigurations[it]
						doProducer(
							producer = clientFactory.createProducer(producerConfiguration),
							producerConfiguration = producerConfiguration,
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
			inactiveReceivers.set(receiverCount)

			log.info("Creating ${destinationConfigurations.size} destinations...")
			destinationConfigurations
				.forEach(clientFactory::createDestination)

		}
	}

	override fun cleanUp(
		configuration: TestConfiguration,
		clientFactory: ClientFactory
	) {
		run {
			testExecutor.shutdown()
			testExecutor.awaitTermination(60, TimeUnit.SECONDS)

			clientFactory.cleanUpDestinations()
		}
	}

	private fun doReceiver(
		receiver: ClientReceiver,
		receiverConfiguration: ReceiverConfiguration,
		receiverId: Int
	): Pair<ArrayList<TimedInterval>, ArrayList<TimedInterval>> {
		val latenciesWithTimestamp = ArrayList<TimedInterval>()
		val intervalWithTimestamp = ArrayList<TimedInterval>()

		fun observe(receivedTime: Long, sentTime: Long, requestTime: Long) {
			val timestamp = timestamp(receivedTime)
			val latency = receivedTime - sentTime
			val receiveInterval = receivedTime - requestTime
			latenciesWithTimestamp.add(timestamp.timingFor(latency, receiverId))
			intervalWithTimestamp.add(timestamp.timingFor(receiveInterval, receiverId))
		}

		log.info("Receiver $receiverId: Starting receiver...")
		receiver.use {
			receiver.start()
			activateReceiver()
			awaitForReceivers()
			log.info("Receiver $receiverId: Receiver started successfully! Will now proceed to test...")
			var receivedAny = false
			val receiverStartTime = time()
			val maxDuration = Duration.ofSeconds(120).toNanos()
			while (
				(activeProducers.get() > 0 || receivedAny) &&
				time() - receiverStartTime <= maxDuration
			) {
				val requestTime = time()
				val message = receiver.receive(receiverConfiguration.queueName, receiverConfiguration)
				val receivedTime = time()
				receivedAny = message != null
				if (message != null) {
					if (latenciesWithTimestamp.size % printCount == 0) {
						log.info("$receiverId: Received ${latenciesWithTimestamp.size} messages...")
					}
					val sentTime = message.bodyAsString().substring(defaultMessage.length).toLong()

					observe(receivedTime, sentTime, requestTime)
					message.ack()
				}
			}
			log.info("Receiver $receiverId: All producers finished their work, stopping...")
		}
		return latenciesWithTimestamp to intervalWithTimestamp
	}

	private fun doProducer(
		producer: ClientProducer,
		producerConfiguration: ProducerConfiguration,
		producerId: Int
	): ArrayList<TimedInterval> {
		val requestIntervalsWithTimestamp = ArrayList<TimedInterval>()
		fun observe(requestTime: Long, producedTime: Long) {
			val timestamp = timestamp(requestTime)
			val interval = producedTime - requestTime
			requestIntervalsWithTimestamp.add(timestamp.timingFor(interval, producerId))
		}

		log.info("$producerId: Starting receiver...")
		producer.use {
			producer.start()
			awaitForReceivers()
			Thread.sleep(5000L)
			log.info("$producerId: Producer started successfully! Will now proceed to test...")
			val maxDuration = Duration.ofSeconds(120).toNanos()
			val producerStartTime = time()
			for (i in 1..messageCount) {
				if (time() - producerStartTime >= maxDuration) {
					break
				}
				if (i % printCount == 0) {
					log.info("$producerId: Produced $i messages...")
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
