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
package terrastore.common;

import java.io.IOException;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class ErrorMessage implements MessagePackable, MessageUnpackable {

    public static final int BAD_REQUEST_ERROR_CODE = 400;
    public static final int FORBIDDEN_ERROR_CODE = 403;
    public static final int NOT_FOUND_ERROR_CODE = 404;
    public static final int CONFLICT_ERROR_CODE = 409;
    public static final int INTERNAL_SERVER_ERROR_CODE = 500;
    public static final int UNAVAILABLE_ERROR_CODE = 503;
    private int code;
    private String message;

    public ErrorMessage(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public ErrorMessage() {
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    protected void setCode(int code) {
        this.code = code;
    }

    protected void setMessage(String message) {
        this.message = message;
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packInt(packer, code);
        if (message != null) {
            MsgPackUtils.packString(packer, message);
        }
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        code = MsgPackUtils.unpackInt(unpacker);
        message = MsgPackUtils.unpackString(unpacker);
    }

    @Override
    public String toString() {
        return "Code: " + code + "\nMessage: " + message;
    }

}
