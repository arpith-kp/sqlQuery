/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.acme;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;



/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		/* Create a tableenvironment using the blink planner and work in flow mode */
		TableEnvironment tEnv = TableEnvironment.create(settings);
//Read SQL file
//		Path path = Paths.get("/Users/z003fwy/workspace/stockproject/sqlQuery/src/main/java/org/acme/userBehavior.sql");
		Path path = Paths.get("/Users/z003fwy/workspace/stockproject/sqlQuery/src/main/java/org/acme/connectKafka.sql");
		List<String> sql = Files.readAllLines(path);
//Distinguish different SQL statements by matching prefixes with regular expressions
		List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
		System.out.println(calls.toString());
//According to different SQL statements, call tableenvironment to execute
		for (SqlCommandParser.SqlCommandCall call : calls) {
			switch (call.command) {
				case SET:
					String key = call.operands[0];
					String value = call.operands[1];
					//Set parameters
					tEnv.getConfig().getConfiguration().setString(key, value);
					break;
				case CREATE_TABLE:
				case INSERT_INTO:
					String ddl = call.operands[0];
					tEnv.sqlUpdate(ddl);
					break;
				default:
					throw new RuntimeException("Unsupported command: " + call.command);
			}
		}
//Submit job
		tEnv.execute("SQL Job");



	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO.
	 */
	public static class Order {
		public Long user;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(Long user, String product, int amount) {
			this.user = user;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + user +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}

		// execute program

	}
}
