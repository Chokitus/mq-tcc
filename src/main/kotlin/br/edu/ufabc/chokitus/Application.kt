package br.edu.ufabc.chokitus

import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.impl.configuration.TestConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.unicast.BatchOpsBenchmark
import br.edu.ufabc.chokitus.benchmark.impl.unicast.SingleOpsBenchmark
import br.edu.ufabc.chokitus.impl.Artemis
import br.edu.ufabc.chokitus.impl.Kafka
import br.edu.ufabc.chokitus.impl.Pulsar
import br.edu.ufabc.chokitus.impl.RabbitMQ
import br.edu.ufabc.chokitus.util.ArgumentParser
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.InputStream
import java.nio.file.Paths
import kotlin.io.path.inputStream
import krangl.DataFrame
import krangl.head
import krangl.readCSV
import org.apache.commons.csv.CSVFormat

val testableClients =
	mapOf(
		"ARTEMIS" to (Artemis.clientFactory() to Artemis.clientProperties()),
		"RABBITMQ" to (RabbitMQ.clientFactory() to RabbitMQ.clientProperties()),
		"PULSAR" to (Pulsar.clientFactory() to Pulsar.clientProperties()),
		"KAFKA" to (Kafka.clientFactory() to Kafka.clientProperties())
	)

fun main(vararg args: String) {
	println("asdas")
	DataFrame.readCSV(
		"C:\\Users\\victo\\Documents\\TCC\\dev\\kotlin\\mq-base-api-kotlin\\untitled\\test_results\\rabbitmq-batch-1c-4p-1d-1000b\\45853465582891\\receiver_latency.csv",
		CSVFormat.newFormat(';')
	)
		.head()
		.let { println(it) }

}

fun runTests(args: Array<out String>) {
	println("Received args: ${args.joinToString(", ")}")

	val arguments = ArgumentParser.parse(args)
	val destination = arguments.destinations
	val consumer = arguments.consumers
	val producer = arguments.producers
	val client = arguments.client
	println("Parsed args are $arguments")

	// 	val destination = 1
	// 	val consumer = 1
	// 	val producer = 1
	// 	val client = "artemis"

	val (clientConstructor, propertiesType) =
		testableClients[client.uppercase()]
			?: throw IllegalArgumentException("Client must be one of ${testableClients.keys}")

	val clientFactory: ClientFactory =
		Paths.get("benchmark/mq/${client.lowercase()}.json")
			.inputStream()
			.let { jacksonObjectMapper().readValue(it, propertiesType.java) }
			.also { println("Successfully parsed client properties as $it") }
			.let { clientConstructor(it, arguments) }

	val testConfiguration: TestConfiguration =
		Paths.get("benchmark/test/${destination}d_${consumer}c_${producer}p.json")
			.inputStream()
			.let<InputStream, TestConfiguration> { jacksonObjectMapper().readValue(it) }
			.also { println("Successfully parsed test properties as $it") }

	// 			./start.sh br --client artemis --consumers 1 --producers 64 --destinations 1 --size 1000 --count 100000

	if (arguments.benchmark.equals("batch", true)) {
		BatchOpsBenchmark(
			arguments = arguments,
			messageSize = arguments.messageSize,
			messageCount = arguments.messageCount,
		)
	} else {
		SingleOpsBenchmark(
			arguments = arguments,
			messageSize = arguments.messageSize,
			messageCount = arguments.messageCount,
		)
	}
		.doBenchmark(
			configuration = testConfiguration,
			clientFactory = clientFactory,
		)
}
