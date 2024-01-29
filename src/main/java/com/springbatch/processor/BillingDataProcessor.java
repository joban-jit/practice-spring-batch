package com.springbatch.processor;

import com.springbatch.record.BillingData;
import com.springbatch.record.ReportingData;
import com.springbatch.service.PricingService;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;

public class BillingDataProcessor implements ItemProcessor<BillingData, ReportingData> {

    private final PricingService pricingService;

    public BillingDataProcessor(PricingService pricingService) {
        this.pricingService = pricingService;
    }

    @Value("${spring.cellular.spending.threshold:150}")
    private float spendingThreshold;

    @Override
    public ReportingData process(BillingData billingData) throws Exception {
        double billingTotal = billingData.dataUsage() * pricingService.getDataPricing() + billingData.callDuration() * pricingService.getCallPricing() + billingData.smsCount() * pricingService.getSmsPricing();
        if (billingTotal < spendingThreshold) {
            return null;
        }
        return new ReportingData(billingData, billingTotal);
    }
}
