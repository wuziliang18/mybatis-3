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
package org.apache.ibatis.executor.keygen;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 */
public class Jdbc3KeyGenerator implements KeyGenerator {

	@Override
	public void processBefore(Executor executor, MappedStatement ms,
			Statement stmt, Object parameter) {
		// do nothing
	}

	@Override
	public void processAfter(Executor executor, MappedStatement ms,
			Statement stmt, Object parameter) {
		processBatch(ms, stmt, getParameters(parameter));
	}

	public void processBatch(MappedStatement ms, Statement stmt,
			Collection<Object> parameters) {
		ResultSet rs = null;
		try {
			rs = stmt.getGeneratedKeys();// 获取主键
			final Configuration configuration = ms.getConfiguration();
			final TypeHandlerRegistry typeHandlerRegistry = configuration
					.getTypeHandlerRegistry();
			final String[] keyProperties = ms.getKeyProperties();// 获取主键
			final ResultSetMetaData rsmd = rs.getMetaData();
			TypeHandler<?>[] typeHandlers = null;
			if (keyProperties != null
					&& rsmd.getColumnCount() >= keyProperties.length) {
				for (Object parameter : parameters) {// 可能是批量插入
					// there should be one row for each statement (also one for
					// each parameter)
					if (!rs.next()) {
						break;
					}
					// 批量插入 每个对象都是唯一的 所以都要new
					final MetaObject metaParam = configuration
							.newMetaObject(parameter);
					// 但是对象类型肯定是一样的 所以下边TypeHandler初始化一次就可以
					if (typeHandlers == null) {
						typeHandlers = getTypeHandlers(typeHandlerRegistry,
								metaParam, keyProperties, rsmd);
					}
					populateKeys(rs, metaParam, keyProperties, typeHandlers);
				}
			}
		} catch (Exception e) {
			throw new ExecutorException(
					"Error getting generated key or setting result to parameter object. Cause: "
							+ e, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// ignore
				}
			}
		}
	}

	/**
	 * 如果参数是Collection的直接返回 如果是map的尝试从key中取出Collection 否则新建一个list放进去参数
	 * 
	 * @param parameter
	 * @return
	 */
	private Collection<Object> getParameters(Object parameter) {
		Collection<Object> parameters = null;
		if (parameter instanceof Collection) {
			parameters = (Collection) parameter;
		} else if (parameter instanceof Map) {
			Map parameterMap = (Map) parameter;
			if (parameterMap.containsKey("collection")) {
				parameters = (Collection) parameterMap.get("collection");
			} else if (parameterMap.containsKey("list")) {
				parameters = (List) parameterMap.get("list");
			} else if (parameterMap.containsKey("array")) {
				parameters = Arrays
						.asList((Object[]) parameterMap.get("array"));
			}
		}
		if (parameters == null) {
			parameters = new ArrayList<Object>();
			parameters.add(parameter);
		}
		return parameters;
	}

	/**
	 * 获取主键的typeHandler
	 * 
	 * @param typeHandlerRegistry
	 * @param metaParam
	 * @param keyProperties
	 * @param rsmd
	 * @return
	 * @throws SQLException
	 */
	private TypeHandler<?>[] getTypeHandlers(
			TypeHandlerRegistry typeHandlerRegistry, MetaObject metaParam,
			String[] keyProperties, ResultSetMetaData rsmd) throws SQLException {
		TypeHandler<?>[] typeHandlers = new TypeHandler<?>[keyProperties.length];
		for (int i = 0; i < keyProperties.length; i++) {
			if (metaParam.hasSetter(keyProperties[i])) {
				Class<?> keyPropertyType = metaParam
						.getSetterType(keyProperties[i]);
				TypeHandler<?> th = typeHandlerRegistry.getTypeHandler(
						keyPropertyType,
						JdbcType.forCode(rsmd.getColumnType(i + 1)));
				typeHandlers[i] = th;
			}
		}
		return typeHandlers;
	}

	/**
	 * 从ResultSet读取主键字段值赋值给MetaObject
	 * 
	 * @param rs
	 * @param metaParam
	 * @param keyProperties主键
	 * @param typeHandlers主键的typehanlder
	 * @throws SQLException
	 */
	private void populateKeys(ResultSet rs, MetaObject metaParam,
			String[] keyProperties, TypeHandler<?>[] typeHandlers)
			throws SQLException {
		for (int i = 0; i < keyProperties.length; i++) {
			TypeHandler<?> th = typeHandlers[i];
			if (th != null) {
				Object value = th.getResult(rs, i + 1);
				metaParam.setValue(keyProperties[i], value);
			}
		}
	}

}
