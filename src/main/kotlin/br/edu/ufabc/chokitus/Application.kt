package br.edu.ufabc.chokitus

import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.impl.configuration.TestConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.unicast.BatchOpsBenchmark
import br.edu.ufabc.chokitus.benchmark.impl.unicast.SingleOpsBenchmark
import br.edu.ufabc.chokitus.impl.Artemis
import br.edu.ufabc.chokitus.impl.Kafka
import br.edu.ufabc.chokitus.impl.Pulsar
import br.edu.ufabc.chokitus.impl.RabbitMQ
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.ArgumentParser
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.io.path.bufferedWriter
import kotlin.io.path.inputStream
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name

val testableClients =
	mapOf(
		"ARTEMIS" to (Artemis.clientFactory() to Artemis.clientProperties()),
		"RABBITMQ" to (RabbitMQ.clientFactory() to RabbitMQ.clientProperties()),
		"PULSAR" to (Pulsar.clientFactory() to Pulsar.clientProperties()),
		"KAFKA" to (Kafka.clientFactory() to Kafka.clientProperties())
	)

fun main(vararg args: String) {
	if (args.contains("--extract")) {
		extractResults()
		return
	}
	runTests(args)
}

fun runTests(args: Array<out String>) {
	println("Received args: ${args.joinToString(", ")}")

	val arguments = ArgumentParser.parse(args)
	val destination = arguments.destinations
	val consumer = arguments.consumers
	val producer = arguments.producers
	val client = arguments.client
	println("Parsed args are $arguments")

	val (clientConstructor, propertiesType) =
		testableClients[client.uppercase()]
			?: throw IllegalArgumentException("Client must be one of ${testableClients.keys}")

	val clientProperties: ClientProperties =
		Paths.get("benchmark/mq/${client.lowercase()}.json")
			.inputStream()
			.let { jacksonObjectMapper().readValue(it, propertiesType.java) }
			.also { println("Successfully parsed client properties as $it") }

	if (arguments.isAll) {
		runAll(clientConstructor, clientProperties, arguments)
		return
	}

	if (arguments.isAllSizes) {
		runAllSizes(clientConstructor, clientProperties, arguments)
		return
	}

	val testConfiguration: TestConfiguration =
		Paths.get("benchmark/test_new/${destination}d_${consumer}c_${producer}p.json")
			.inputStream()
			.let<InputStream, TestConfiguration>(jacksonObjectMapper()::readValue)
			.also { println("Successfully parsed test properties as $it") }

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
			clientFactory = clientConstructor(clientProperties, arguments),
		)
}

fun runAllSizes(
	clientConstructor: (ClientProperties, ArgumentParser.ParseResult) -> ClientFactory,
	clientProperties: ClientProperties,
	arguments: ArgumentParser.ParseResult
) {
	val testConfiguration: TestConfiguration =
		Paths.get("benchmark/test_new/1d_2c_2p.json")
			.inputStream()
			.let<InputStream, TestConfiguration>(jacksonObjectMapper()::readValue)
			.also { println("Successfully parsed test properties as $it") }

	val messageSizes = listOf(125, 250, 1000, 2000, 4000, 8000, 16000)
	listOf("single", "batch")
		.asSequence()
		.flatMap { bench -> messageSizes.map { bench to it } }
		.forEach { (benchmark, messageSize) ->
			val isSingle = benchmark == "single"
			val newArguments = arguments.copy(
				benchmark = benchmark,
				batchSize = 1.takeIf { isSingle } ?: arguments.batchSize,
				messageSize = messageSize
			)
			if (isSingle) {
				SingleOpsBenchmark(
					arguments = newArguments,
					messageSize = newArguments.messageSize,
					messageCount = newArguments.messageCount,
				).doBenchmark(
					configuration = testConfiguration,
					clientFactory = clientConstructor(clientProperties, newArguments)
				)
			} else {
				BatchOpsBenchmark(
					arguments = newArguments,
					messageSize = newArguments.messageSize,
					messageCount = newArguments.messageCount,
				).doBenchmark(
					configuration = testConfiguration,
					clientFactory = clientConstructor(clientProperties, newArguments)
				)
			}
		}
}

fun runAll(
	clientConstructor: (ClientProperties, ArgumentParser.ParseResult) -> ClientFactory,
	clientProperties: ClientProperties,
	arguments: ArgumentParser.ParseResult
) {
	val jacksonObjectMapper = jacksonObjectMapper()
	Paths.get("benchmark/test_new")
		.let { it.toFile().listFiles()!! }
		.filter { it.isFile }
		.filter { !it.name.startsWith("benchmark_template") }
		.map { it to jacksonObjectMapper.readValue<TestConfiguration>(it) }
		.forEach { (file, configuration) ->
			println("Executing tests...")
			val newArguments = arguments.copyParsing(file).copy(
				benchmark = "single",
				batchSize = 1
			)
			SingleOpsBenchmark(
				arguments = newArguments,
				messageSize = newArguments.messageSize,
				messageCount = newArguments.messageCount,
			).doBenchmark(
				configuration = configuration,
				clientFactory = clientConstructor(clientProperties, newArguments)
			)

			val batchArguments = newArguments.copy(
				benchmark = "batch",
				batchSize = arguments.batchSize
			)
			BatchOpsBenchmark(
				arguments = batchArguments,
				messageSize = batchArguments.messageSize,
				messageCount = batchArguments.messageCount,
			).doBenchmark(
				configuration = configuration,
				clientFactory = clientConstructor(clientProperties, batchArguments)
			)
		}
}

fun extractResults() {
	val extract = "receiver_latency"
	Paths.get("test_results/3").listDirectoryEntries()
		.filter { it.isDirectory() }
		.flatMap { it.listDirectoryEntries() }
		.filter { it.isDirectory() }
		.map { folder -> folder.listDirectoryEntries().find { it.name.startsWith(extract) }!! }
		.stream()
		.flatMap { Files.lines(it).skip(1) }
		.let { stream ->
			Paths.get("test_results/$extract.csv").bufferedWriter().use { writer ->
				writer.write(
					"timestamp;normal_timestamp;client;producers;consumers;message_size;benchmark;" +
							"destinations;max_latency;min_latency;avg_latency;latency_std;balance;quantity\n"
				)
				stream.forEach {
					writer.write(it.replace(",", "."))
					writer.newLine()
				}
			}
		}
}
