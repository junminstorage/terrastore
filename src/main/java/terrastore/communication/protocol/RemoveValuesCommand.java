package terrastore.communication.protocol;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;

import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sven Johansson
 */
public class RemoveValuesCommand extends AbstractCommand<Map<Key, Value>> {

    private String bucketName;
    private Set<Key> keys;
    private boolean conditional;
    private Predicate predicate;
    
    public RemoveValuesCommand(String bucketName, Set<Key> keys) {
        this.bucketName = bucketName;
        this.keys = keys;
        this.conditional = false;
        this.predicate = null;
    }
    
    public RemoveValuesCommand(String bucketName, Set<Key> keys, Predicate predicate) {
        this.bucketName = bucketName;
        this.keys = keys;
        this.conditional = true;
        this.predicate = predicate;
    }
    
    public RemoveValuesCommand(RemoveValuesCommand command, Set<Key> nodeKeys) {
        this.bucketName = command.bucketName;
        this.conditional = command.conditional;
        this.predicate = command.predicate;
        this.keys = keys;
    }

    @Override
    public Response executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucketName, keys);
        Map<Key, Value> result = new HashMap<Key, Value>();
        for (Map.Entry<Node, Set<Key>> nodeToKeysEntry : nodeToKeys.entrySet()) {
            Node node = nodeToKeysEntry.getKey();
            Set<Key> nodeKeys = nodeToKeysEntry.getValue();
            RemoveValuesCommand command = new RemoveValuesCommand(this, nodeKeys);
            result.putAll(node.<Map<Key, Value>>send(command));
        }
        return new ValuesResponse(id, result);
    }

    @Override
    public Response executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        Map<Key, Value> result = new HashMap<Key, Value>();
        if (bucket != null) {
            if (!conditional) {
                for (Key key : keys) {
                    bucket.remove(key);
                    result.put(key, new Value("removed".getBytes()));
                }
            } else {
                for (Key key : keys) {
                    Value value = bucket.conditionalGet(key, predicate);
                    if (value != null) {
                        bucket.remove(key);
                        result.put(key, new Value("removed".getBytes()));
                    }
                }
            }
        }
        return new ValuesResponse(id, result);
    }
    
    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        bucketName = MsgPackUtils.unpackString(unpacker);
        keys = MsgPackUtils.unpackKeys(unpacker);
        conditional = MsgPackUtils.unpackBoolean(unpacker);
        predicate = MsgPackUtils.unpackPredicate(unpacker);
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, bucketName);
        MsgPackUtils.packKeys(packer, keys);
        MsgPackUtils.packBoolean(packer, conditional);
        MsgPackUtils.packPredicate(packer, predicate);
    }

  
}
