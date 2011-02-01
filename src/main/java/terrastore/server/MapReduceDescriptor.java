/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.server;

import terrastore.common.ErrorMessage;
import terrastore.store.Key;

/**
 * @author Sergio Bossa
 */
public class MapReduceDescriptor {

    public Range range;
    public Task task;

    public MapReduceDescriptor(Range range, Task task) {
        this.range = range;
        this.task = task;
    }

    public MapReduceDescriptor() {
    }

    public void sanitize() throws ServerOperationException {
        if (range != null) {
            range.sanitize();
        }
        if (task != null) {
            task.sanitize();
        } else {
            ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No task information provided!");
            throw new ServerOperationException(error);
        }
    }

    public static class Range {

        public Key startKey;
        public Key endKey;
        public String comparator;
        public long timeToLive;

        public Range(Key startKey, Key endKey, String comparator, Long timeToLive) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.comparator = comparator;
            this.timeToLive = timeToLive;
        }

        public Range() {
        }

        public void sanitize() throws ServerOperationException {
            if (startKey == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No startKey provided!");
                throw new ServerOperationException(error);
            }
            if (comparator == null) {
                comparator = "";
            }
        }

    }

    public static class Task {

        public String mapper;
        public String combiner;
        public String reducer;
        public long timeout;
        public Parameters parameters;

        public Task(String mapper, String combiner, String reducer, Long timeout, Parameters parameters) {
            this.mapper = mapper;
            this.combiner = combiner;
            this.reducer = reducer;
            this.timeout = timeout;
            this.parameters = parameters;
        }

        public Task() {
        }

        public void sanitize() throws ServerOperationException {
            if (mapper == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No mapper provided!");
                throw new ServerOperationException(error);
            }
            if (reducer == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No reducer provided!");
                throw new ServerOperationException(error);
            }
            if (combiner == null) {
                combiner = reducer;
            }
            if (timeout == 0) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Timeout value must be greater than zero!");
                throw new ServerOperationException(error);
            }
            if (parameters == null) {
                parameters = new Parameters();
            }
        }

    }
}
