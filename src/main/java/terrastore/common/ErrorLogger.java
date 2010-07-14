/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.common;

import org.slf4j.Logger;

/**
 * @author Sergio Bossa
 */
public class ErrorLogger {

    private static final int USER_ERROR_FAMILY = 400;
    private static final int SERVER_ERROR_FAMILY = 500;

    public static void LOG(Logger logger, ErrorMessage message, Throwable cause) {
        if (message.getCode() >= USER_ERROR_FAMILY && message.getCode() < SERVER_ERROR_FAMILY) {
            logger.info(message.getMessage(), cause);
        } else if (message.getCode() >= SERVER_ERROR_FAMILY) {
            logger.error(message.getMessage(), cause);
        }
    }
}
