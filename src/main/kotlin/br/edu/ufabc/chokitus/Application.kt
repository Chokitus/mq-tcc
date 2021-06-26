package br.edu.ufabc.chokitus

import br.edu.ufabc.chokitus.benchmark.impl.unicast.SingleDestinationSingleOpsBenchmark
import br.edu.ufabc.chokitus.benchmark.impl.configuration.SingleDestinationConfiguration
import br.edu.ufabc.chokitus.impl.Pulsar

fun main() {
	val pulsarProperties = Pulsar.PulsarProperties(
		serviceURL = "pulsar://localhost:6650"
	)
	val testConfiguration = SingleDestinationConfiguration(
		receiverCount = 2,
		producerCount = 1,
		messageCount = 1,
		messageSize = 100
	)
	SingleDestinationSingleOpsBenchmark()
		.doBenchmark(
			configuration = testConfiguration,
			clientFactory = Pulsar.PulsarClientFactory(pulsarProperties)
		).let(::println)
}
