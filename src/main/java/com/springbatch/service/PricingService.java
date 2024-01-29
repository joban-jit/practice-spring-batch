package com.springbatch.service;

import java.util.Random;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;

public class PricingService {

    @Value("${spring.cellular.pricing.data:0.01}")
    private float dataPricing;

    @Getter
    @Value("${spring.cellular.pricing.call:0.5}")
    private float callPricing;

    @Getter
    @Value("${spring.cellular.pricing.sms:0.1}")
    private float smsPricing;

    private final Random random = new Random();

    public float getDataPricing() {
        if (this.random.nextInt(1000) % 7 == 0) {
            throw new PricingException("Error while retrieving data pricing");
        }
        return this.dataPricing;
    }

}

