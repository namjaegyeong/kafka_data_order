package com.example.kafkatoy.controller

import com.example.kafkatoy.service.Consumer
import com.example.kafkatoy.service.Producer
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class ProducerController(private val producer: Producer, private val consumer: Consumer) {

    @GetMapping("/")
    fun dataOrderGuarantee() {
        producer.dataOrderGuaranteeProduce()
        consumer.dataOrderGuaranteeConsumer()
    }
}