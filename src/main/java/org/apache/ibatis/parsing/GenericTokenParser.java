/**
 *    Copyright 2009-2015 the original author or authors.
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
package org.apache.ibatis.parsing;

/**
 * @author Clinton Begin
 */
public class GenericTokenParser {

  private final String openToken;
  private final String closeToken;
  private final TokenHandler handler;

  public GenericTokenParser(String openToken, String closeToken, TokenHandler handler) {
    this.openToken = openToken;
    this.closeToken = closeToken;
    this.handler = handler;
  }

  public String parse(String text) {
    final StringBuilder builder = new StringBuilder();
    final StringBuilder expression = new StringBuilder();//临时保存包含正确openToken开始到结束的部分
    if (text != null && text.length() > 0) {
      char[] src = text.toCharArray();
      int offset = 0;//下一次开始的位置
      // search open token
      int start = text.indexOf(openToken, offset);//找到openToken开始的地方
      while (start > -1) {
        if (start > 0 && src[start - 1] == '\\') {//如果有\\注释的openToken不处理 直接输出
          // this open token is escaped. remove the backslash and continue.
          builder.append(src, offset, start - offset - 1).append(openToken);
          offset = start + openToken.length();
        } else {
          // found open token. let's search close token.
          expression.setLength(0);//重置
          builder.append(src, offset, start - offset);//拼接非token包含的部分
          offset = start + openToken.length();
          int end = text.indexOf(closeToken, offset);
          while (end > -1) {
            if (end > offset && src[end - 1] == '\\') {
              // this close token is escaped. remove the backslash and continue.
              expression.append(src, offset, end - offset - 1).append(closeToken);
              offset = end + closeToken.length();
              end = text.indexOf(closeToken, offset);
            } else {
              expression.append(src, offset, end - offset);
              offset = end + closeToken.length();
              break;
            }
          }
          if (end == -1) {//没有close token
            // close token was not found.
            builder.append(src, start, src.length - start);
            offset = src.length;
          } else {
            builder.append(handler.handleToken(expression.toString()));
            offset = end + closeToken.length();
          }
        }
        start = text.indexOf(openToken, offset);//循环
      }
      if (offset < src.length) {//有剩余的补上
        builder.append(src, offset, src.length - offset);
      }
    }
    return builder.toString();
  }
}
