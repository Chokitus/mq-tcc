package br.edu.ufabc.chokitus.benchmark.data

data class TimedInterval(
	val timestamp: Long,
	val interval: Long
) {
	var classification: Int = -1
}

infix fun Long.timing(interval: Long) = TimedInterval(this, interval)
