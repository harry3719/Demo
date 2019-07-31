package com.db.hcl.routes;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.remote.RemoteFile;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MyRoutes extends RouteBuilder {

	
	private static final Logger log = LoggerFactory.getLogger(MyRoutes.class);

	@Override
	public void configure() throws Exception {
		
		ExecutorService executor = Executors.newFixedThreadPool(10);
		
		
		// TODO Auto-generated method stub
//		from("sftp://192.168.0.106:22/?" + "username=harry1&" + "password=harry1&" + "useUserKnownHostsFile=false" +"&disconnect=true" + "&maxMessagesPerPoll=3") 
//		.streamCaching()
//		
//		.log("Timer is triggred")
////		.split().tokenize("\n").streaming()
//		.process(new Processor() {
//			
//			@Override
//			public void process(Exchange exchange) throws Exception {
//				// TODO Auto-generated method stub
//	             System.out.println("****************************Processinggg****************************" + exchange); 
//
//              if (exchange != null && exchange.getException() == null) { 
//              System.out.println("Fetched the file from sftp is ::::::::::: "+exchange.getIn().getBody());
//
////            exchange.getOut().setBody(exchange.getIn().getBody(File.class)); 
//            
//          } else { 
//             System.out.println("****************************EXCHANGE IS NULL****************************"); 
//          } 
//			}
//		})
//		.to("file://c:/input");
		
		
		
//		from("file:src/main/resources/data/parallel-file-processing?noop=true")
//	    .threads(10)
//	    .process(new PreProcessor())
//	    .aggregate(constant(true), new ArrayListAggregationStrategy())
//	    .completionFromBatchConsumer()
//	    .process(new PostProcessor());
//		initialDelay=1000&readLock=changed&readLockDeleteOrphanLockFiles=false&maxMessagesPerPoll=3&move=done&moveFailed=error&readLockCheckInterval=5000&readLockTimeout=50000
//		&readLock=changed&readLockTimeout=100&readLockCheckInterval=20&maxMessagesPerPoll=3
		
		from("sftp://127.0.0.1:22/?" + "username=harry1&" + "password=harry1&" + "useUserKnownHostsFile=false" +"&strictHostKeyChecking=no"
				+ "&disconnect=true" + "&streamDownload=true" + "&localWorkDirectory=F:\\LocalFileDir"
				+ "&move=.success" + "&moveFailed=.failed" + "&initialDelay=1000&readLock=changed&readLockDeleteOrphanLockFiles=false"
				+ "&maxMessagesPerPoll=3&readLockCheckInterval=5000&readLockTimeout=50000")
						.threads(4)
						.log("Processing thread : ${threadName}")
						.marshal()
						.pgp("file:C:\\Users\\Harry\\Desktop\\keys\\harry_pgp_public_key.asc" , "harry <harry3719@gmail.com>")
						.to("file:F:\\harry_out\\Locakl_encFile_marshld")
						.to("seda:harry1");
	    
		from("seda:harry1?concurrentConsumers=4")
				.log("Processing thread : ${threadName}")
				.unmarshal()
				.pgp("file:C:\\Users\\Harry\\Desktop\\keys\\harry_pgp_private_key.asc" , "harry <harry3719@gmail.com>", "harry3719")
				.to("file:F:\\harry_out\\Locakl_encFile_unmarshld")
//				.process(new PreProcessor())
				.marshal()
				.pgp("file:C:\\Users\\Harry\\Desktop\\keys\\dbbank_pgp_public_key.asc", "dbbank <dbbank@gmail.com>", "dbbank")
		//	    .aggregate(constant(true), new ArrayListAggregationStrategy())
		//	    .completionFromBatchConsumer()
		//	    .parallelProcessing() // <- for parallel processing
				
//				"sftp://127.0.0.1:22/?" + "username=harry1&" + "password=harry1&" + "useUserKnownHostsFile=false" +"&strictHostKeyChecking=no"
//				+ "&disconnect=true" + "&streamDownload=true" + "&localWorkDirectory=F:\\LocalFileDir"
//				+ "&move=.success" + "&moveFailed=.failed" + "&initialDelay=1000&readLock=changed&readLockDeleteOrphanLockFiles=false"
//				+ "&maxMessagesPerPoll=3&readLockCheckInterval=5000&readLockTimeout=50000"
				.to("sftp://127.0.0.1:22/?" + "username=harryOut&" + "password=harry1&" + "doneFileName=${file:name}.conf")
		//	    .process(new PostProcessor())
	    ;
		
		
	}
	private static class PostProcessor implements Processor {
	    @SuppressWarnings("unchecked")
	    @Override
		public void process(final Exchange exchange) throws Exception {
			final Object body = exchange.getIn().getBody();

			final List<RemoteFile<?>> list = (List<RemoteFile<?>>) body;
			for (final RemoteFile<?> genericFile : list) {
//	            LOG.info("file = " + genericFile.getAbsoluteFilePath());

				System.out.println("file = " + genericFile.getAbsoluteFilePath());
			}
		}
	}
	
	private static class PreProcessor implements Processor {
		
	    @Override
	    public void process(final Exchange exchange) throws Exception {
	        final Object body = exchange.getIn().getBody();
//	            LOG.info("file = " + genericFile.getAbsoluteFilePath());
	        	
	        	System.out.println("PreProcessor incoming body is ::: = " + body);
	    }
	}
}

//simply combines Exchange body values into an ArrayList<Object>
 class ArrayListAggregationStrategy implements AggregationStrategy {
  public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
      Object newBody = newExchange.getIn().getBody();
      List<Object> list = null;
      if (oldExchange == null) {
          list = new ArrayList<Object>();
          list.add(newBody);
          newExchange.getIn().setBody(list);
          System.out.println("newExchange value is ::::: "+ newExchange);
          return newExchange;
      } else {
          list = oldExchange.getIn().getBody(List.class);
          list.add(newBody);
          System.out.println("Old Exchange value is ::::: "+ oldExchange);
          return oldExchange;
      }
  }
}
