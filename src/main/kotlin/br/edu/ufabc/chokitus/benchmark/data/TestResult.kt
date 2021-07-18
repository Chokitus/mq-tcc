package br.edu.ufabc.chokitus.benchmark.data

data class TestResult(
	val latenciesAndReceiveIntervals: List<Pair<List<TimedInterval>, List<TimedInterval>>>,
	val produceIntervals: List<List<TimedInterval>>
) {

	fun flattenSortedLatencyAndReceive(): Pair<List<TimedInterval>, List<TimedInterval>> =
		latenciesAndReceiveIntervals
			.fold(
				listOf<TimedInterval>() to listOf<TimedInterval>()
			) { (accLat, accRec), (lat, rec) ->
				(accLat + lat) to (accRec + rec)
			}
			.let { (lat, rec) -> lat.sortedBy { it.timestamp } to rec.sortedBy { it.timestamp } }

}
