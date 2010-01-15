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
package terrastore.communication.protocol;

import java.io.Serializable;
import java.util.Map;
import terrastore.common.ErrorMessage;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class Response implements Serializable {

    private static final long serialVersionUID = 12345678901L;

    private final String correlationId;
    private final Map<String, Value> entries;
    private final ErrorMessage error;

    public Response(String correlationId, Map<String, Value> entries) {
        this(correlationId, entries, null);
    }

    public Response(String correlationId, ErrorMessage error) {
        this(correlationId, null, error);
    }

    protected Response(String correlationId, Map<String, Value> entries, ErrorMessage error) {
        this.correlationId = correlationId;
        this.entries = entries;
        this.error = error;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Map<String, Value> getEntries() {
        return entries;
    }

    public ErrorMessage getError() {
        return error;
    }

    public boolean isOk() {
        return error == null;
    }
}
