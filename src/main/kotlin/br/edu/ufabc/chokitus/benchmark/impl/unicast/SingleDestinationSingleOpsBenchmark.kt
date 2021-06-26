package br.edu.ufabc.chokitus.benchmark.impl.unicast

import br.edu.ufabc.chokitus.benchmark.AbstractBenchmark
import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.impl.configuration.SingleDestinationConfiguration
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import com.google.common.collect.ConcurrentHashMultiset
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.allOf
import java.util.concurrent.CompletableFuture.runAsync
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class SingleDestinationSingleOpsBenchmark :
	AbstractBenchmark<SingleDestinationConfiguration, Unit>() {

	private lateinit var testExecutor: ExecutorService

	private var testStartTime: Long = 0
	private var testEndTime: Long = 0
	private val latencies = ConcurrentHashMap<Long, ConcurrentHashMultiset<Long>>()

	companion object {
		private const val NANO_TO_SECOND = 1_000_000_000L
	}

	private val message: String = UUID.randomUUID().toString()

	private val messageSize = message.toInt()

	private val queue: String = UUID.randomUUID().toString()

	private val activeProducers = AtomicInteger(0)

	override fun doBenchmarkImpl(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		clientFactory.start()

		testStartTime = System.nanoTime()

		val receiver: List<CompletableFuture<Void>> = (1..configuration.receiverCount).map {
			runAsync({ doReceiver(clientFactory.createReceiver(), configuration) }, testExecutor)
		}

		val producers: List<CompletableFuture<Void>> = (1..configuration.producerCount).map {
			runAsync({ doProducer(clientFactory.createProducer(), configuration) }, testExecutor)
		}

		allOf(
			*(receiver + producers).toTypedArray()
		).get()

		testEndTime = System.nanoTime()
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
			testExecutor.shutdown()
			testExecutor.awaitTermination(60, TimeUnit.SECONDS)
		}
	}

	private fun doReceiver(
		receiver: AbstractReceiver<*, *, *>,
		configuration: SingleDestinationConfiguration
	) {
		receiver.start()
		while (activeProducers.get() > 0) {
			receiver.receive(queue)?.let { message ->
				val receivedTime = System.nanoTime()
				val sentTime = message.body().decodeToString().substring(messageSize).toLong()

				observe(receivedTime, sentTime)
				message.ack()
			}
		}
	}

	private fun doProducer(
		producer: AbstractProducer<*, *, *>,
		configuration: SingleDestinationConfiguration
	) {
		producer.start()
		repeat(configuration.messageCount) {
			producer.produce(queue, "$message${System.nanoTime()}".encodeToByteArray())
		}
		activeProducers.decrementAndGet()
	}

	private fun observe(receivedTime: Long, sentTime: Long) {
		latencies.getOrPut((receivedTime - testStartTime) / NANO_TO_SECOND) {
			ConcurrentHashMultiset.create()
		}
			.add(receivedTime - sentTime)
	}

}
