package com.example.kafkatoy.service

import com.example.kafkatoy.domain.KafkaMessage
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.Future

@Service
class Producer(private val kafkaProducerTemplate: KafkaTemplate<String, KafkaMessage>) {

    fun produce(topic: String, kafkaMessage: KafkaMessage) {
        kafkaProducerTemplate.send(topic, kafkaMessage)
    }

    fun dataOrderGuaranteeProduce(){
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "172.17.2.13:9092,172.17.2.12:9092,172.17.2.14:9092"
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        val producer: KafkaProducer<String, String> = KafkaProducer<String, String>(properties)

        val randomString = RandomStringUtils.randomAlphanumeric(2940);
        val randomStringLength = randomString.length + 60

        for (i in 1..100) {
            val producerRecord : ProducerRecord<String, String> = ProducerRecord("throughput-test1", "key", "Consume data from kafka : Order = $i, Size = $randomStringLength, Message = $randomString")
            val future: Future<RecordMetadata> = producer.send(producerRecord)!!
            val result = future.get()
            println("Produce data from kafka : Offset =" + result.offset() + ", Size =" + result.serializedValueSize())
        }
        producer.flush()
        producer.close()
    }
}