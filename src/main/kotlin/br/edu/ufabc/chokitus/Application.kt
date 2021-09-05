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
import kotlin.math.log
import kotlin.math.sqrt
import kotlin.random.Random

val testableClients =
	mapOf(
		"ARTEMIS" to (Artemis.clientFactory() to Artemis.clientProperties()),
		"RABBITMQ" to (RabbitMQ.clientFactory() to RabbitMQ.clientProperties()),
		"PULSAR" to (Pulsar.clientFactory() to Pulsar.clientProperties()),
		"KAFKA" to (Kafka.clientFactory() to Kafka.clientProperties())
	)

fun main(vararg args: String) {
	extractResults()
	// 	runTests(args)
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

	val clientProperties: ClientProperties =
		Paths.get("benchmark/mq/${client.lowercase()}.json")
			.inputStream()
			.let { jacksonObjectMapper().readValue(it, propertiesType.java) }
			.also { println("Successfully parsed client properties as $it") }

	if (arguments.isAll) {
		runAll(clientConstructor, clientProperties, arguments)
		return
	}

	val testConfiguration: TestConfiguration =
		Paths.get("benchmark/test_new/${destination}d_${consumer}c_${producer}p.json")
			.inputStream()
			.let<InputStream, TestConfiguration>(jacksonObjectMapper()::readValue)
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
			clientFactory = clientConstructor(clientProperties, arguments),
		)
}

fun runAll(
	clientConstructor: (ClientProperties, ArgumentParser.ParseResult) -> ClientFactory,
	clientProperties: ClientProperties,
	arguments: ArgumentParser.ParseResult
) {
	val jacksonObjectMapper = jacksonObjectMapper()
	Paths.get("benchmark/test_new")
		.also { println(it.toAbsolutePath()) }
		.let {
			it.toFile().listFiles()!!
		}
		.filter { it.isFile }
		.filter { !it.name.startsWith("benchmark_template") }
		.map { it to jacksonObjectMapper.readValue<TestConfiguration>(it) }
		.map { (file, configuration) ->
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
		.flatMap {
			it.listDirectoryEntries()
		}
		.filter { it.isDirectory() }
		.map { it.listDirectoryEntries().find { it.name.startsWith(extract) }!! }
		.stream()
		.flatMap {
			Files.lines(it).skip(1)
		}
		.let { stream ->
			Paths.get("test_results/$extract.csv").bufferedWriter().use { writer ->
				writer.write("timestamp;normal_timestamp;client;producers;consumers;message_size;benchmark;destinations;max_latency;min_latency;avg_latency;latency_std;balance;quantity\n")
				stream.forEach {
					writer.write(it.replace(",", "."))
					writer.newLine()
				}
			}
		}
}

fun fakeData() {
	val clients = listOf("artemis", "rabbitmq", "pulsar", "kafka")
	val type = listOf("batch", "single")
	val producers = listOf(1, 4, 16, 64)
	val consumers = listOf(1, 2, 4)
	val destinations = listOf(1, 2, 4)
	val ms = 1000
	val messageCount = 1000000

	val expectedAverage = 8.0

	val writer = Paths.get("fake.csv").bufferedWriter()
	writer.write("Timestamp;Cliente;Produtores;Consumidores;Tamanho da Mensagem;Tipo de Benchmark;Numero de Destinos;Latencia Maxima;Latencia Minima;Latencia Media;Desvio Padrao da Latencia;Fator de balanceamento;Quantidade Trafegada\n")
	for (cli in clients) {
		for (p in producers) {
			for (c in consumers) {
				for (d in destinations) {
					for (t in type) {
						val timestampAmount =
							messageCount /
									((messageCount / 500) *
											(1 + log(p.toFloat(), 4.toFloat()) / 6) *
											(1 + c / 6)) / 10

						println(timestampAmount)

						for (ts in 0..timestampAmount.toInt()) {
							val avg = expectedAverage + (Random.nextInt(0, 20) / 10.0 - 1)
							val maxAvg = avg * Random.nextInt(1, 4).toDouble()
							val minAvg = avg / Random.nextInt(1, 4).toDouble()
							val std = sqrt(maxAvg - minAvg)
							val dif = Random.nextInt(0, 10 * (p - 1) + 1) / (100.0 * p)
							val bs = 1 - dif
							val msgs = Random.nextInt(1000, 2000)
							val csv =
								"$ts;$cli;$p;$c;$ms;$t;$d;$maxAvg;$minAvg;$avg;$std;$bs;$msgs"
							writer.write(csv)
							writer.newLine()
						}
					}
				}
			}
		}
	}
	writer.close()
}
