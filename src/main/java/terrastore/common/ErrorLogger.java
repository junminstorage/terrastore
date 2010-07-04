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
