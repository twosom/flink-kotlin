package source

import extention.tuple
import model.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.security.SecureRandom
import java.util.*

class SensorSource
    : RichParallelSourceFunction<SensorReading>() {

    private var running: Boolean = true
    private lateinit var random: Random

    override fun open(
        parameters: Configuration,
    ) {
        this.random = SecureRandom()
    }

    override fun run(
        ctx: SourceContext<SensorReading>,
    ) {
        val taskIdx = this.runtimeContext.indexOfThisSubtask

        var curFTemp =
            (1..10).map { "sensor_${taskIdx * 10 + it}" tuple (65 + this.random.nextGaussian() * 20) }

        while (running) {
            curFTemp = curFTemp.map { it.f0 tuple (it.f1 + (random.nextGaussian() * 0.5)) }
            val curTime = Calendar.getInstance().timeInMillis
            curFTemp.forEach { ctx.collect(SensorReading(it.f0, curTime, it.f1)) }
            Thread.sleep(100)
        }
    }

    override fun cancel() {
        this.running = false
    }

}