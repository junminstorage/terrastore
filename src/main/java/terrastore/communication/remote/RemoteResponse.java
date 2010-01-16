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
package terrastore.communication.remote;

import java.io.Serializable;
import terrastore.common.ErrorMessage;

/**
 * @author Sergio Bossa
 */
public class RemoteResponse implements Serializable {

    private static final long serialVersionUID = 12345678901L;

    private final String correlationId;
    private final Object result;
    private final ErrorMessage error;

    public RemoteResponse(String correlationId, Object result) {
        this(correlationId, result, null);
    }

    public RemoteResponse(String correlationId, ErrorMessage error) {
        this(correlationId, null, error);
    }

    protected RemoteResponse(String correlationId, Object result, ErrorMessage error) {
        this.correlationId = correlationId;
        this.result = result;
        this.error = error;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Object getResult() {
        return result;
    }

    public ErrorMessage getError() {
        return error;
    }

    public boolean isOk() {
        return error == null;
    }
}
