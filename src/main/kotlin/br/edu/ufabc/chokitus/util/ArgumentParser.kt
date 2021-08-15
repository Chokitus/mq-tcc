package br.edu.ufabc.chokitus.util

object ArgumentParser {

	data class ParseResult(
		val client: String,
		val consumers: Int,
		val producers: Int,
		val destinations: Int,
		val messageSize: Int,
		val messageCount: Int,
		val benchmark: String,
		val batchSize: Int
	) {

		fun type() =
			"batch".takeIf { benchmark.equals("batch", true) }
				?: "single"

		fun isBatch() =
			type() == "batch"

		override fun toString(): String =
			"$client-${type()}-${consumers}c-${producers}p-${destinations}d-${messageSize}b"
	}

	fun parse(args: Array<out String>): ParseResult {
		val values = mutableMapOf<String, String>()

		for (i in args.indices) {
			val arg = args[i]
			if (arg.startsWith("--")) {
				if (i == args.size - 1 || args[i + 1].startsWith("--")) {
					throw IllegalArgumentException("$arg does not have a value")
				}
				values[arg.substring(2)] = args[i + 1]
			}
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
