package org.apache.flink.runtime.execution;

/**
 * Thrown to trigger a stopping of the executing task. Intended to cause a more graceful shutdown
 * than failing and canceling in job manager side.
 */
public class StopTaskException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StopTaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public StopTaskException(Throwable cause) {
        super(cause);
    }

    public StopTaskException(String message) {
        super(message);
    }

    public StopTaskException() {
        super("task stopped");
    }
}
