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
package jp.classmethod.aws.gradle.lambda;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import lombok.Getter;
import lombok.Setter;

import org.gradle.api.GradleException;
import org.gradle.api.internal.ConventionTask;
import org.gradle.api.tasks.TaskAction;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.CreateFunctionRequest;
import com.amazonaws.services.lambda.model.CreateFunctionResult;
import com.amazonaws.services.lambda.model.FunctionCode;
import com.amazonaws.services.lambda.model.GetFunctionRequest;
import com.amazonaws.services.lambda.model.GetFunctionResult;
import com.amazonaws.services.lambda.model.ResourceNotFoundException;
import com.amazonaws.services.lambda.model.Runtime;
import com.amazonaws.services.lambda.model.UpdateFunctionCodeRequest;
import com.amazonaws.services.lambda.model.UpdateFunctionCodeResult;
import com.amazonaws.services.lambda.model.UpdateFunctionConfigurationRequest;
import com.amazonaws.services.lambda.model.UpdateFunctionConfigurationResult;
import com.amazonaws.services.lambda.model.VpcConfig;

public class AWSLambdaMigrateFunctionTask extends ConventionTask {
	
	
	@Getter
	@Setter
	private String functionName;
	
	@Getter
	@Setter
	private String role;
	
	@Getter
	@Setter
	private Runtime runtime = Runtime.Nodejs;
	
	@Getter
	@Setter
	private String handler;
	
	@Getter
	@Setter
	private String functionDescription;
	
	@Getter
	@Setter
	private Integer timeout;
	
	@Getter
	@Setter
	private Integer memorySize;
	
	@Getter
	@Setter
	private File zipFile;
	
	@Getter
	@Setter
	private S3File s3File;
	
	@Getter
	@Setter
	private VpcConfigWrapper vpc;
	
	@Getter
	@Setter
	private Boolean publish;
	
	@Getter
	private CreateFunctionResult createFunctionResult;
	
	
	public AWSLambdaMigrateFunctionTask() {
		setDescription("Create / Update Lambda function.");
		setGroup("AWS");
	}
	
	@TaskAction
	public void createOrUpdateFunction() throws FileNotFoundException, IOException {
		// to enable conventionMappings feature
		String functionName = getFunctionName();
		File zipFile = getZipFile();
		S3File s3File = getS3File();
		
		if (functionName == null)
			throw new GradleException("functionName is required");
		
		if ((zipFile == null && s3File == null) || (zipFile != null && s3File != null)) {
			throw new GradleException("exactly one of zipFile or s3File is required");
		}
		if (s3File != null) {
			s3File.validate();
		}
		
		AWSLambdaPluginExtension ext = getProject().getExtensions().getByType(AWSLambdaPluginExtension.class);
		AWSLambda lambda = ext.getClient();
		
		try {
			GetFunctionResult getFunctionResult =
					lambda.getFunction(new GetFunctionRequest().withFunctionName(functionName));
			updateStack(lambda, getFunctionResult);
		} catch (ResourceNotFoundException e) {
			getLogger().warn(e.getMessage());
			getLogger().warn("Creating function... {}", functionName);
			createFunction(lambda);
		}
	}
	
	private void createFunction(AWSLambda lambda) throws IOException {
		// to enable conventionMappings feature
		File zipFile = getZipFile();
		S3File s3File = getS3File();
		
		FunctionCode functionCode;
		if (zipFile != null) {
			try (RandomAccessFile raf = new RandomAccessFile(getZipFile(), "r");
					FileChannel channel = raf.getChannel()) {
				MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
				buffer.load();
				functionCode = new FunctionCode().withZipFile(buffer);
			}
		} else {
			// assume s3File is not null
			functionCode = new FunctionCode()
				.withS3Bucket(s3File.getBucketName())
				.withS3Key(s3File.getKey())
				.withS3ObjectVersion(s3File.getObjectVersion());
		}
		CreateFunctionRequest request = new CreateFunctionRequest()
			.withFunctionName(getFunctionName())
			.withRuntime(getRuntime())
			.withRole(getRole())
			.withHandler(getHandler())
			.withDescription(getFunctionDescription())
			.withTimeout(getTimeout())
			.withMemorySize(getMemorySize())
			.withPublish(getPublish())
			.withVpcConfig(getVpcConfig())
			.withCode(functionCode);
		createFunctionResult = lambda.createFunction(request);
		getLogger().info("Create Lambda function requested: {}", createFunctionResult.getFunctionArn());
	}
	
	private void updateStack(AWSLambda lambda, GetFunctionResult getFunctionResult) throws IOException {
		updateFunctionCode(lambda);
		updateFunctionConfiguration(lambda, getFunctionResult);
	}
	
	private void updateFunctionCode(AWSLambda lambda) throws IOException {
		// to enable conventionMappings feature
		File zipFile = getZipFile();
		S3File s3File = getS3File();
		
		UpdateFunctionCodeRequest request = new UpdateFunctionCodeRequest()
			.withFunctionName(getFunctionName());
		if (zipFile != null) {
			try (RandomAccessFile raf = new RandomAccessFile(getZipFile(), "r");
					FileChannel channel = raf.getChannel()) {
				MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
				buffer.load();
				request = request.withZipFile(buffer);
			}
		} else {
			// assume s3File is not null
			request = request
				.withS3Bucket(s3File.getBucketName())
				.withS3Key(s3File.getKey())
				.withS3ObjectVersion(s3File.getObjectVersion());
		}
		UpdateFunctionCodeResult updateFunctionCode = lambda.updateFunctionCode(request);
		getLogger().info("Update Lambda function requested: {}", updateFunctionCode.getFunctionArn());
	}
	
	private void updateFunctionConfiguration(AWSLambda lambda, GetFunctionResult getFunctionResult) {
		UpdateFunctionConfigurationRequest request = new UpdateFunctionConfigurationRequest()
			.withFunctionName(getFunctionName())
			.withRole(getRole())
			.withHandler(getHandler())
			.withDescription(getFunctionDescription())
			.withTimeout(getTimeout())
			.withVpcConfig(getVpcConfig())
			.withMemorySize(getMemorySize());
		UpdateFunctionConfigurationResult updateFunctionConfiguration = lambda.updateFunctionConfiguration(request);
		getLogger().info("Update Lambda function configuration requested: {}",
				updateFunctionConfiguration.getFunctionArn());
	}
	
	private VpcConfig getVpcConfig() {
		if (getVpc() != null) {
			return getVpc().toVpcConfig();
		}
		return null;
	}
}
