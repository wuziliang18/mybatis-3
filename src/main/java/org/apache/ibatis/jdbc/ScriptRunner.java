/**
 *    Copyright 2009-2016 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.jdbc;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * @author Clinton Begin
 */
public class ScriptRunner {

	private static final String LINE_SEPARATOR = System.getProperty(
			"line.separator", "\n");

	private static final String DEFAULT_DELIMITER = ";";

	private Connection connection;

	private boolean stopOnError;// 出错后是否停止运行
	private boolean throwWarning;
	private boolean autoCommit;// 是否自动提交
	private boolean sendFullScript;// 是否一次执行整个脚本命令
	private boolean removeCRs;// 是否替换\r\n为\n
	private boolean escapeProcessing = true;// 逃逸字符开关

	private PrintWriter logWriter = new PrintWriter(System.out);
	private PrintWriter errorLogWriter = new PrintWriter(System.err);

	private String delimiter = DEFAULT_DELIMITER;
	private boolean fullLineDelimiter = false;

	public ScriptRunner(Connection connection) {
		this.connection = connection;
	}

	public void setStopOnError(boolean stopOnError) {
		this.stopOnError = stopOnError;
	}

	public void setThrowWarning(boolean throwWarning) {
		this.throwWarning = throwWarning;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public void setSendFullScript(boolean sendFullScript) {
		this.sendFullScript = sendFullScript;
	}

	public void setRemoveCRs(boolean removeCRs) {
		this.removeCRs = removeCRs;
	}

	/**
	 * @since 3.1.1
	 */
	public void setEscapeProcessing(boolean escapeProcessing) {
		this.escapeProcessing = escapeProcessing;
	}

	public void setLogWriter(PrintWriter logWriter) {
		this.logWriter = logWriter;
	}

	public void setErrorLogWriter(PrintWriter errorLogWriter) {
		this.errorLogWriter = errorLogWriter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void setFullLineDelimiter(boolean fullLineDelimiter) {
		this.fullLineDelimiter = fullLineDelimiter;
	}

	public void runScript(Reader reader) {
		setAutoCommit();

		try {
			if (sendFullScript) {// 全部运行
				executeFullScript(reader);
			} else {// 单条处理
				executeLineByLine(reader);
			}
		} finally {
			rollbackConnection();
		}
	}

	private void executeFullScript(Reader reader) {
		StringBuilder script = new StringBuilder();
		try {
			BufferedReader lineReader = new BufferedReader(reader);
			String line;
			while ((line = lineReader.readLine()) != null) {
				script.append(line);
				script.append(LINE_SEPARATOR);
			}
			String command = script.toString();
			println(command);
			executeStatement(command);
			commitConnection();
		} catch (Exception e) {
			String message = "Error executing: " + script + ".  Cause: " + e;
			printlnError(message);
			throw new RuntimeSqlException(message, e);
		}
	}

	/**
	 * 按行处理 遇到分隔符后执行前边所有命令 最后会空 如果不为空说明分隔符有问题 会抛出异常
	 * 
	 * @param reader
	 */
	private void executeLineByLine(Reader reader) {
		StringBuilder command = new StringBuilder();
		try {
			BufferedReader lineReader = new BufferedReader(reader);
			String line;
			while ((line = lineReader.readLine()) != null) {
				command = handleLine(command, line);
			}
			commitConnection();
			checkForMissingLineTerminator(command);
		} catch (Exception e) {
			String message = "Error executing: " + command + ".  Cause: " + e;
			printlnError(message);
			throw new RuntimeSqlException(message, e);
		}
	}

	public void closeConnection() {
		try {
			connection.close();
		} catch (Exception e) {
			// ignore
		}
	}

	/**
	 * 设置connection的autoCommit
	 */
	private void setAutoCommit() {
		try {
			if (autoCommit != connection.getAutoCommit()) {
				connection.setAutoCommit(autoCommit);
			}
		} catch (Throwable t) {
			throw new RuntimeSqlException("Could not set AutoCommit to "
					+ autoCommit + ". Cause: " + t, t);
		}
	}

	/**
	 * 不是自动提交的话 提交事务
	 */
	private void commitConnection() {
		try {
			if (!connection.getAutoCommit()) {
				connection.commit();
			}
		} catch (Throwable t) {
			throw new RuntimeSqlException(
					"Could not commit transaction. Cause: " + t, t);
		}
	}

	/**
	 * 不是自动提交的话回滚
	 */
	private void rollbackConnection() {
		try {
			if (!connection.getAutoCommit()) {
				connection.rollback();
			}
		} catch (Throwable t) {
			// ignore
		}
	}

	private void checkForMissingLineTerminator(StringBuilder command) {
		if (command != null && command.toString().trim().length() > 0) {
			throw new RuntimeSqlException(
					"Line missing end-of-line terminator (" + delimiter
							+ ") => " + command);
		}
	}

	/**
	 * 按行处理
	 * 
	 * @param command
	 * @param line
	 * @return
	 * @throws SQLException
	 * @throws UnsupportedEncodingException
	 */
	private StringBuilder handleLine(StringBuilder command, String line)
			throws SQLException, UnsupportedEncodingException {
		String trimmedLine = line.trim();
		if (lineIsComment(trimmedLine)) {// 注释行
			final String cleanedString = trimmedLine.substring(2).trim()
					.replaceFirst("//", "");
			if (cleanedString.toUpperCase().startsWith("@DELIMITER")) {// 重新定义结尾分隔符
				delimiter = cleanedString.substring(11, 12);
				return command;
			}
			println(trimmedLine);
		} else if (commandReadyToExecute(trimmedLine)) {// 遇到结尾分隔符开始执行前边的命令
														// 执行命令后清空
			command.append(line.substring(0, line.lastIndexOf(delimiter)));
			command.append(LINE_SEPARATOR);
			println(command);
			executeStatement(command.toString());
			command.setLength(0);
		} else if (trimmedLine.length() > 0) {// 追加命令知道满足上一个判断 到达分隔符去执行
			command.append(line);
			command.append(LINE_SEPARATOR);
		}
		return command;
	}

	/**
	 * 该行是否是注释
	 * 
	 * @param trimmedLine
	 * @return
	 */
	private boolean lineIsComment(String trimmedLine) {
		return trimmedLine.startsWith("//") || trimmedLine.startsWith("--");
	}

	/**
	 * 如果是整个执行 且命令中包括分割符号 开始执行 或者 是按行执行 且该行就是分割符号 开始执行
	 * 
	 * @param trimmedLine
	 * @return
	 */
	private boolean commandReadyToExecute(String trimmedLine) {
		// issue #561 remove anything after the delimiter
		return !fullLineDelimiter && trimmedLine.contains(delimiter)
				|| fullLineDelimiter && trimmedLine.equals(delimiter);
	}

	/**
	 * jdbc执行命令
	 * 
	 * @param command
	 * @throws SQLException
	 */
	private void executeStatement(String command) throws SQLException {
		boolean hasResults = false;
		Statement statement = connection.createStatement();
		statement.setEscapeProcessing(escapeProcessing);
		String sql = command;
		if (removeCRs) {
			sql = sql.replaceAll("\r\n", "\n");
		}
		if (stopOnError) {
			hasResults = statement.execute(sql);
			if (throwWarning) {
				SQLWarning warning = statement.getWarnings();
				if (warning != null) {
					throw warning;
				}
			}
		} else {
			try {
				hasResults = statement.execute(sql);
			} catch (SQLException e) {
				String message = "Error executing: " + command + ".  Cause: "
						+ e;
				printlnError(message);
			}
		}
		printResults(statement, hasResults);
		try {
			statement.close();
		} catch (Exception e) {
			// Ignore to workaround a bug in some connection pools
		}
	}

	/**
	 * 输出结果
	 * 
	 * @param statement
	 * @param hasResults
	 */
	private void printResults(Statement statement, boolean hasResults) {
		try {
			if (hasResults) {
				ResultSet rs = statement.getResultSet();
				if (rs != null) {
					ResultSetMetaData md = rs.getMetaData();
					int cols = md.getColumnCount();
					for (int i = 0; i < cols; i++) {
						String name = md.getColumnLabel(i + 1);
						print(name + "\t");
					}
					println("");
					while (rs.next()) {
						for (int i = 0; i < cols; i++) {
							String value = rs.getString(i + 1);
							print(value + "\t");
						}
						println("");
					}
				}
			}
		} catch (SQLException e) {
			printlnError("Error printing results: " + e.getMessage());
		}
	}

	private void print(Object o) {
		if (logWriter != null) {
			logWriter.print(o);
			logWriter.flush();
		}
	}

	private void println(Object o) {
		if (logWriter != null) {
			logWriter.println(o);
			logWriter.flush();
		}
	}

	private void printlnError(Object o) {
		if (errorLogWriter != null) {
			errorLogWriter.println(o);
			errorLogWriter.flush();
		}
	}

}
