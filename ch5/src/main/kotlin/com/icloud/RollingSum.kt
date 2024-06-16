package com.icloud

import extention.tuple
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object RollingSum {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val inputStream = env.fromElements(
            1 tuple 2 tuple 2,
            2 tuple 3 tuple 1,
            2 tuple 2 tuple 4,
            1 tuple 5 tuple 3
        )

        val resultStream = inputStream.keyBy { it.f0 }
            .sum(0)

        resultStream.print()

        env.execute("Rolling Sum Example")
    }
}