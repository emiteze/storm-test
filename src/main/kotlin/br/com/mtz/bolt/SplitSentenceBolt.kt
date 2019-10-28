package br.com.mtz.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class SplitSentenceBolt : BaseBasicBolt() {

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val sentence: String = tuple.getString(0)
        sentence.split(" ").forEach {
            collector.emit(Values(it))
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("word"))
    }

}