package br.com.mtz.spout

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Values
import org.apache.storm.utils.Utils
import java.util.Random

class RandomWordSpout : BaseRichSpout() {

    lateinit var collector: SpoutOutputCollector
    lateinit var random: Random

    override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext, collector: SpoutOutputCollector) {
        this.collector = collector
        this.random = Random()
    }

    override fun nextTuple() {
        Utils.sleep(100)
        val words: List<String> = listOf("mateus", "zup", "musica", "teste", "kotlin")
        val word = words[random.nextInt(words.size)]
        this.collector.emit(Values(word))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("word"))
    }

}