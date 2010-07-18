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
package terrastore.store;

import java.io.Serializable;
import org.terracotta.modules.annotations.InstrumentedClass;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;

/**
 * Generic value object contained by {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
@InstrumentedClass
public interface Value extends Serializable {

    public byte[] getBytes();

    public Value dispatch(Key key, Update update, Function function);

    public boolean dispatch(Key key, Predicate predicate, Condition condition);
}
