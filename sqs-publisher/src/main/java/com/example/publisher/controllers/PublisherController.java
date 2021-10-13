package com.example.publisher.controllers;

import com.example.publisher.services.SQSEventPublisher;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PublisherController {

    @Autowired
    private SQSEventPublisher sqsEventPublisher;

    @PostMapping("/sendMessage")
    public ResponseEntity sendMessage(@RequestBody JsonNode jsonNode) {
        sqsEventPublisher.publishEvent(jsonNode);
        return ResponseEntity.ok().build();
    }
}
