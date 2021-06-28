package br.edu.ufabc.chokitus.benchmark.impl.unicast

import br.edu.ufabc.chokitus.benchmark.AbstractBenchmark
import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.ClientReceiver
import br.edu.ufabc.chokitus.benchmark.impl.configuration.SingleDestinationConfiguration
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import com.google.common.collect.ConcurrentHashMultiset
import java.util.UUID
import java.util.concurrent.CompletableFuture.allOf
import java.util.concurrent.CompletableFuture.runAsync
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class SingleDestinationBatchOpsBenchmark :
	AbstractBenchmark<SingleDestinationConfiguration>() {

	private lateinit var testExecutor: ExecutorService
	private var startTime: Long = 0

	companion object {
		private const val NANO_TO_SECOND = 1_000_000_000L
	}

	private val message: String = UUID.randomUUID().toString()
	private val messageSize = message.toInt()

	private val queue: String = UUID.randomUUID().toString()

	private val activeProducers = AtomicInteger(0)

	private val latencies = ConcurrentHashMap<Long, ConcurrentHashMultiset<Long>>()

	override fun doBenchmarkImpl(
		configuration: SingleDestinationConfiguration,
		clientFactory: ClientFactory
	) {
		clientFactory.start()

		startTime = System.nanoTime()

		val receiver = (1..configuration.receiverCount).map {
			runAsync({ doReceiver(clientFactory.createReceiver(), configuration) }, testExecutor)
		}

		val producers = (1..configuration.producerCount).map {
			runAsync({ doProducer(clientFactory.createProducer(), configuration) }, testExecutor)
		}

		allOf(
			*(receiver + producers).toTypedArray()
		).get()
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
			testExecutor.shutdown()
			testExecutor.awaitTermination(60, TimeUnit.SECONDS)

			clientFactory.deleteQueue(queue)
		}
	}

	private fun doReceiver(
		receiver: ClientReceiver,
		configuration: SingleDestinationConfiguration
	) {
		receiver.start()
		while (activeProducers.get() > 0) {
			val messages: List<AbstractMessage> = receiver.receiveBatch(queue)
			val receivedTime = System.nanoTime()

			messages.forEach { message ->
				val sentTime = message.body().decodeToString().substring(messageSize).toLong()

				observe(receivedTime, sentTime)
			}

			// 			receiver.ackAll(messages)
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
		latencies.getOrPut((receivedTime - startTime) / NANO_TO_SECOND) {
			ConcurrentHashMultiset.create()
		}
			.add(receivedTime - sentTime)
	}

}
