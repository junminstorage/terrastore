package terrastore.decorator.failure;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author Sergio Bossa
 */
public class BackoffFailureHandler<T> implements InvocationHandler {

    private final T delegate;
    private final int maxRetries;
    private final long backoffDelay;

    private BackoffFailureHandler(T delegate, int maxRetries, long backoffDelay) {
        this.delegate = delegate;
        this.maxRetries = maxRetries;
        this.backoffDelay = backoffDelay;
    }

    public static <T> T newInstance(T delegate, Class<T> type, int maxRetries, long backoffDelay) {
        return (T) Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[]{type},
                new BackoffFailureHandler<T>(delegate, maxRetries, backoffDelay));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        HandleFailure failure = method.getAnnotation(HandleFailure.class);
        if (failure != null) {
            int currentRetry = 0;
            while (true) {
                try {
                    return invokeDelegate(method, args);
                } catch (InvocationTargetException ex) {
                    if (ex.getCause().getClass().isAssignableFrom(failure.exception())) {
                        currentRetry++;
                        backoffOrThrowException(currentRetry, ex.getCause());
                    } else {
                        throw ex.getCause();
                    }
                }
            }
        } else {
            return invokeDelegate(method, args);
        }
    }

    private void backoffOrThrowException(int currentRetry, Throwable ex) throws Throwable {
        if (currentRetry <= maxRetries) {
            try {
                Thread.sleep(backoffDelay);
            } catch (InterruptedException ignored) {
            }
        } else {
            throw ex;
        }
    }

    private Object invokeDelegate(Method method, Object[] args) throws IllegalAccessException, InvocationTargetException, IllegalArgumentException, SecurityException {
        method.setAccessible(true);
        return method.invoke(delegate, args);
    }
}
