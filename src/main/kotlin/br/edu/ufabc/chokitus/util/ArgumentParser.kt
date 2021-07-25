package br.edu.ufabc.chokitus.util

import java.lang.IllegalArgumentException

object ArgumentParser {

	data class ParseResult(
		val client: String,
		val consumers: Int,
		val producers: Int,
		val destinations: Int
	)

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
			client = require(values, "client"),
			consumers = require(values, "consumers").toInt(),
			producers = require(values, "producers").toInt(),
			destinations = require(values, "destinations").toInt(),
		)
	}

	private fun require(map: Map<String, String>, value: String): String =
		map[value]
			?: throw IllegalArgumentException("--$value option is required.")

}
