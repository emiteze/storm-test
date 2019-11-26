package br.com.mtz.bolt

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

    private lateinit var onlineInput: HashMap<String, Int>
    private lateinit var model: ArrayList<String>

    private fun Tuple.wentFromTopic(): Boolean = this.fields.contains("topic")

    private fun Tuple.fromTopic(topicName: String): Boolean = this.wentFromTopic() && topicName == this.getStringByField("topic")

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {

        var word: String

        if(tuple.fromTopic("minas.current.model")) {
            word = tuple.getStringByField("value")
            model.add(word)
        } else if(tuple.fromTopic("minas.online.input")) {
            word = tuple.getStringByField("value")
            var count = onlineInput[word]
            if (count == null) count = 0
            onlineInput[word] = ++count
            if(onlineInput[word] == 5) {
                collector.emit(Values(word))
            }
        }

        /*
        if(tuple.sourceComponent == Constants.SYSTEM_COMPONENT_ID && tuple.sourceStreamId == Constants.SYSTEM_TICK_STREAM_ID) {
            counts.keys.forEach { word ->
                val count = counts[word]
                collector.emit(Values("$word $count"))
                log.info("Emitting a count of {} for word {}", count , word)
            }
        } else {
            val word = tuple.getStringByField("value").split(" ")[0]
            var count = counts[word]
            if(count == null) count = 0
            count++
            counts[word] = count
            log.info("Incrementing a count of {} for word {}", count , word)
        }
        */
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("message"))
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext) {
        super.prepare(stormConf, context)
        onlineInput = HashMap()
        model = ArrayList()
    }

}