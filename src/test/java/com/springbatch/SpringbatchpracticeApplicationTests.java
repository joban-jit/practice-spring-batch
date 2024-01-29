package com.springbatch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootTest
@SpringBatchTest
//@ExtendWith(OutputCaptureExtension.class)
class SpringbatchpracticeApplicationTests {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private JobRepositoryTestUtils jobRepositoryTestUtils;

	@Autowired
	private JdbcTemplate jdbcTemplate;

//	@Autowired
//	private Job job;
//
//	@Autowired
//	private JobLauncher jobLauncher;

	@BeforeEach
	public void setup(){
		this.jobRepositoryTestUtils.removeJobExecutions();
		JdbcTestUtils.deleteFromTables(jdbcTemplate, "billing_data");
	}

	@Test
	void testJobExecution() throws Exception {
//	void testJobExecution(CapturedOutput output) throws Exception {
//		JobParameters jobParameters = new JobParametersBuilder()
//				.addString("input.file", "/some/input/file")
//				.addString("file.format", "csv", false)
//				.toJobParameters();

		JobParameters jobParameters = this.jobLauncherTestUtils.getUniqueJobParametersBuilder()
				.addString("input.file", "input/billing-2023-01.csv")
				.addString("file.format", "csv", false)
				.addString("output.file", "staging/billing-report-2023-01.csv")
				.addJobParameter("data.year", 2023, Integer.class)
				.addJobParameter("data.month", 1, Integer.class)
				.toJobParameters();
		Path billingReport = Paths.get("staging", "billing-report-2023-01.csv");
//		JobExecution jobExecution = this.jobLauncher.run(this.job, jobParameters);
		JobExecution jobExecution = this.jobLauncherTestUtils.launchJob(jobParameters);
		Assertions.assertTrue(Files.exists(Paths.get("staging", "billing-2023-01.csv")));
//		Assertions.assertTrue(output.getOut().contains("processing billing information from file /some/input/file"));
		Assertions.assertEquals(0, ExitStatus.COMPLETED.compareTo(jobExecution.getExitStatus()));
		Assertions.assertEquals(1000, JdbcTestUtils.countRowsInTable(jdbcTemplate, "billing_data"));
		Assertions.assertTrue(Files.exists(billingReport));
		Assertions.assertEquals(781, Files.lines(billingReport).count());
	}

}
