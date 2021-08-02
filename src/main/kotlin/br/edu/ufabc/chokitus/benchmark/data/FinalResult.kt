package br.edu.ufabc.chokitus.benchmark.data

import java.math.BigDecimal

class FinalResult(
	val timestamp: Long,
	val avg: Double,
	val max: Double,
	val min: Double,
	val count: Int,
	val std: Double,
	val load: Double,
)