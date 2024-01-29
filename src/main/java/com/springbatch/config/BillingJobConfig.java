package com.springbatch.config;

import com.springbatch.exception.PricingException;
import com.springbatch.listener.BillingDataSkipListener;
import com.springbatch.processor.BillingDataProcessor;
import com.springbatch.record.BillingData;
import com.springbatch.record.ReportingData;
import com.springbatch.service.PricingService;
import com.springbatch.tasklet.FilePreparationTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.support.JdbcTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BillingJobConfig {

//    @Bean
//    public Job job(JobRepository jobRepository){
//        return new NotNeededAnymoreBillingJob(jobRepository);
//    }

    @Bean
    public Step step1(JobRepository jobRepository, JdbcTransactionManager jdbcTransactionManager){
        return new StepBuilder("filePreparation", jobRepository)
                .tasklet(new FilePreparationTasklet(), jdbcTransactionManager)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<BillingData> billingDataFileReader(
            @Value("#{jobParameters['input.file']}") String inputFile
    ){
        return new FlatFileItemReaderBuilder<BillingData>()
                .name("billingDataFileReader1")
                .resource(new FileSystemResource(inputFile)) //  .resource(new FileSystemResource("staging/billing-2023-01.csv"))
                .delimited()
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<BillingData> billingDataTableWriter(DataSource dataSource){
        String sql = "insert into billing_data values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(sql)
                .beanMapped()
                .build();
    }

    @Bean
    @StepScope
    public BillingDataSkipListener skipListener(
            @Value("#{jobParameters['skip.file']}") String skippedFile
    ){
        return new BillingDataSkipListener(skippedFile);
    }

    @Bean
    public Step step2(JobRepository jobRepository, JdbcTransactionManager jdbcTransactionManager,
                      ItemReader<BillingData> billingDataFileReader, ItemWriter<BillingData> billingDataTableWriter,
                      BillingDataSkipListener skipListener
                      ){
        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(1000, jdbcTransactionManager)
                .reader(billingDataFileReader)
                .writer(billingDataTableWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(10)
                .listener(skipListener)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<BillingData> billingDataTableReader(
            DataSource dataSource,
            @Value("#{jobParameters['data.year']}") int dataYear,
            @Value("#{jobParameters['data.month']}") int dataMonth
    ){
        String sql = "select * from billing_data where data_year="+dataYear+" and data_month="+dataMonth;
        return new JdbcCursorItemReaderBuilder<BillingData>()
                .name("billingDataTableReader1")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                .build();
    }

    @Bean
    public BillingDataProcessor billingDataProcessor(PricingService pricingService){
        return new BillingDataProcessor(pricingService);
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<ReportingData> billingDataFileWriter(
            @Value("#{jobParameters['output.file']}") String outputFile
    ){
        return new FlatFileItemWriterBuilder<ReportingData>()
                .resource(new FileSystemResource(outputFile))
                .name("billingDataFileWriter1")
                .delimited()
                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
                .build();

    }

    @Bean
    public Step step3(JobRepository jobRepository,
                      JdbcTransactionManager jdbcTransactionManager,
                      ItemReader<BillingData> billingDataTableReader,
                      ItemProcessor<BillingData, ReportingData> billingDataProcessor,
                      ItemWriter<ReportingData> billingDataFileWriter){
        return new StepBuilder("reportGeneration", jobRepository)
                .<BillingData, ReportingData> chunk(100, jdbcTransactionManager)
                .reader(billingDataTableReader)
                .processor(billingDataProcessor)
                .writer(billingDataFileWriter)
                .faultTolerant()
                .retry(PricingException.class)
                .retryLimit(100)
                .build();
    }


    @Bean
    public PricingService pricingService() {
        return  new PricingService();
    }



    @Bean
    public Job job(JobRepository jobRepository, Step step1, Step step2, Step step3){
        return new JobBuilder("billingJob", jobRepository)
                .start(step1)
                .next(step2)
                .next(step3)
                .build();
    }


}
