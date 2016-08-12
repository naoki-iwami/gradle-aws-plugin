/*
 * Copyright 2013-2016 Classmethod, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.classmethod.aws.gradle.s3;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.gradle.api.GradleException;
import org.gradle.api.internal.ConventionTask;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.TaskAction;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Created by iwami on 2016/08/12.
 */
public class DownloadTask extends ConventionTask {
	
	
	@Getter
	@Setter
	private String bucketName;
	
	@Getter
	@Setter
	private String prefix = "";
	
	@Getter
	@Setter
	private File dest;
	
	@Getter
	@Setter
	private int threads = 5;
	
	
	@TaskAction
	public void uploadAction() throws InterruptedException {
		// to enable conventionMappings feature
		String bucketName = getBucketName();
		String prefix = getPrefix();
		File dest = getDest();
		
		if (bucketName == null)
			throw new GradleException("bucketName is not specified");
		if (dest == null)
			throw new GradleException("dest is not specified");
		if (dest.isDirectory() == false)
			throw new GradleException("dest must be directory");
		
		prefix = prefix.startsWith("/") ? prefix.substring(1) : prefix;
		
		AmazonS3PluginExtension ext = getProject().getExtensions().getByType(AmazonS3PluginExtension.class);
		AmazonS3 s3 = ext.getClient();
		download(s3, prefix);
	}
	
	private void download(AmazonS3 s3, String prefix) throws InterruptedException {
		// to enable conventionMappings feature
		String bucketName = getBucketName();
		File dest = getDest();
		
		ExecutorService es = Executors.newFixedThreadPool(threads);
		getLogger().info("Start downloading");
		getLogger().info("downloading... s3://{}/{} to {}", bucketName, prefix, dest.getAbsolutePath());
		
		ObjectListing listing = s3.listObjects(bucketName, prefix);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		for (int i = 0; i < summaries.size(); i++) {
			S3ObjectSummary summary = summaries.get(i);
			es.execute(new InnerDownloadThread(s3, bucketName, prefix, dest, getLogger()));
		}
		
		es.shutdown();
		es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		getLogger().info("Finish downloading");
	}
	
	
	private static class InnerDownloadThread implements Runnable {
		
		
		private AmazonS3 s3;
		
		private String bucketName;
		
		private String s3path;
		
		private File destFile;
		
		private Logger logger;
		
		
		public InnerDownloadThread(AmazonS3 s3, String bucketName, String s3path,
				File destFile, Logger logger) {
			this.s3 = s3;
			this.bucketName = bucketName;
			this.s3path = s3path;
			this.destFile = destFile;
			this.logger = logger;
		}
		
		@Override
		public void run() {
			// to enable conventionMappings feature
			
			logger.info("s3://{}/{} => {}", bucketName, s3path, destFile.getAbsolutePath());
			S3Object object = s3.getObject(bucketName, s3path);
			try {
				byte[] bs = IOUtils.toByteArray(object.getObjectContent());
				FileUtils.writeByteArrayToFile(destFile, bs);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
}
