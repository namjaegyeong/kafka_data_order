package com.example.kafkatoy.service

import com.example.kafkatoy.domain.KafkaMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.*

@Service
class Consumer {
    private var messageList = listOf<KafkaMessage>()

    fun dataOrderGuaranteeConsume(){
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "172.17.2.13:9092,172.17.2.12:9092,172.17.2.14:9092"
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        properties[ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS] = StringDeserializer::class.java
        properties[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS]= StringDeserializer::class.java
        properties[ConsumerConfig.GROUP_ID_CONFIG] = "throughput-test1"

        val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(properties)
        consumer.subscribe(Collections.singletonList("throughput-test1"))

        var message: String? = null
        try {
            val records = consumer.poll(Duration.ofMillis(100000))
            for (record in records) {
                println("Consume data from kafka : Offset = " + record.offset() + ", Size = " + record.serializedValueSize() + " bytes")
            }
        } catch (e: Exception) {
            println(e)
        } finally {
            consumer.close()
        }
    }
}
