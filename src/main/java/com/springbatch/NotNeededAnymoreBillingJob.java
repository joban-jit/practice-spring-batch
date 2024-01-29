package com.springbatch;

public class NotNeededAnymoreBillingJob
//        implements Job
{
//
//    private JobRepository jobRepository;
//
//    public BillingJob(JobRepository jobRepository){
//        this.jobRepository = jobRepository;
//    }
//
//    @Override
//    public String getName() {
//        return "Billing-Job";
//    }
//
//    @Override
//    public void execute(JobExecution jobExecution) {
//        try {
//            JobParameters jobParameters = jobExecution.getJobParameters();
//            String inputFile = jobParameters.getString("input.file");
//            System.out.println("processing billing information from file "+inputFile);
//            jobExecution.setStatus(BatchStatus.COMPLETED);
//            jobExecution.setExitStatus(ExitStatus.COMPLETED);
//
//        }catch (Exception exception){
//            jobExecution.addFailureException(exception);
//            jobExecution.setStatus(BatchStatus.COMPLETED);
//            jobExecution.setExitStatus(ExitStatus.FAILED.addExitDescription(exception.getMessage()));
//        }finally {
//            this.jobRepository.update(jobExecution);
//        }
//
//    }
}
