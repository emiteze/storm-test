package br.com.mtz.bolt

import org.apache.storm.Config
import org.apache.storm.Constants
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

class WordCountBolt : BaseBasicBolt() {

    private val log = LoggerFactory.getLogger(this.javaClass)

    private lateinit var counts: HashMap<String, Int>
    private val emitFrequency: Int = 1

    override fun getComponentConfiguration(): MutableMap<String, Any> {
        val conf: Config = Config()
        conf[Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS] = emitFrequency
        return conf
    }

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        if(tuple.sourceComponent == Constants.SYSTEM_COMPONENT_ID && tuple.sourceStreamId == Constants.SYSTEM_TICK_STREAM_ID) {
            counts.keys.forEach { word ->
                val count = counts[word]
                collector.emit(Values(word, count))
                log.info("Emitting a count of {} for word {}", count , word)
            }
        } else {
            val word = tuple.getString(0)
            var count = counts[word]
            if(count == null) count = 0
            count++
            counts[word] = count
            log.info("Incrementing a count of {} for word {}", count , word)
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("word", "count"))
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext) {
        super.prepare(stormConf, context)
        counts = HashMap()
    }

}