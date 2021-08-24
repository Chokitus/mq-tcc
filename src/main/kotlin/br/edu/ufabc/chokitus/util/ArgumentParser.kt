package br.edu.ufabc.chokitus.util

import java.io.File

object ArgumentParser {

	data class ParseResult(
		val client: String,
		val consumers: Int,
		val producers: Int,
		val destinations: Int,
		val messageSize: Int,
		val messageCount: Int,
		val benchmark: String,
		val batchSize: Int,
		val isAll: Boolean = false
	) {

		companion object {
			fun createAll(
				client: String,
				messageCount: Int,
				messageSize: Int
			) =
				ParseResult(
					client = client,
					consumers = 0,
					producers = 0,
					destinations = 0,
					messageSize = messageSize,
					messageCount = messageCount,
					benchmark = "",
					batchSize = 100,
					isAll = true,
				)

		}

		fun type() =
			"batch".takeIf { benchmark.equals("batch", true) }
				?: "single"

		fun isBatch() =
			type() == "batch"

		override fun toString(): String =
			"$client-${type()}-${consumers}c-${producers}p-${destinations}d-${messageSize}b"

		fun copyParsing(file: File): ParseResult {
			val pattern = Regex("\\.json|d|c|p")
			val (destinations, consumers, producers) = file.name.run {
				replace(pattern, "")
					.split("_")
					.map { it.toInt() }
			}

			return copy(
				destinations = destinations,
				consumers = consumers,
				producers = producers
			)
		}
	}

	fun parse(args: Array<out String>): ParseResult {
		val values = mutableMapOf<String, String>()

		for (i in args.indices) {
			val arg = args[i]
			if (arg.startsWith("--")) {
				val all = arg == "--all" || arg == "--a"
				if (all) {
					values[arg.substring(2)] = ""
					continue
				}
				val doesntHaveNext = i == args.size - 1 || args[i + 1].startsWith("--")
				if (doesntHaveNext) {
					throw IllegalArgumentException("$arg does not have a value")
				}
				values[arg.substring(2)] = args[i + 1]
			}
		}

		if (values.containsKey("all") || values.containsKey("a")) {
			return ParseResult.createAll(
				client = require(values, "client", "cli"),
				messageCount = require(values, "count", "ct").toInt(),
				messageSize = require(values, "size", "s").toInt()
			)
		}

		return ParseResult(
			client = require(values, "client", "cli"),
			consumers = require(values, "consumers", "c").toInt(),
			producers = require(values, "producers", "p").toInt(),
			destinations = require(values, "destinations", "d").toInt(),
			messageSize = require(values, "size", "s").toInt(),
			messageCount = require(values, "count", "ct").toInt(),
			benchmark = require(values, "benchmark", "b"),
			batchSize = require(values, "bs").toInt(),
		)
	}

	private fun require(map: Map<String, String>, value: String, alt: String = ""): String =
		map[value]
			?: map[alt]
			?: throw IllegalArgumentException(
				"--$value${" or --$alt".takeIf { alt.isNotEmpty() } ?: ""} option is required."
			)

}
