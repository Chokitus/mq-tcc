package br.edu.ufabc.chokitus

import br.edu.ufabc.chokitus.benchmark.impl.configuration.SingleDestinationConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.unicast.SingleDestinationSingleOpsBenchmark
import br.edu.ufabc.chokitus.impl.Artemis

fun main() {
	val artemisClientFactory =
		Artemis.ArtemisClientFactory(
			properties = Artemis.ArtemisProperties(
				serverLocatorURL = "tcp://localhost:61616",
				username = "mq-test",
				password = "mq-test",
				ackBatchSize = 10
			)
		)
	val testConfiguration = SingleDestinationConfiguration(
		receiverCount = 2,
		producerCount = 1,
		messageCount = 100000,
		messageSize = 100
	)
	SingleDestinationSingleOpsBenchmark()
		.doBenchmark(
			configuration = testConfiguration,
			clientFactory = artemisClientFactory
		).let(::println)

}
