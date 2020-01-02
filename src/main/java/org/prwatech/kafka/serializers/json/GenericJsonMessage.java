package org.prwatech.kafka.serializers.json;

public class GenericJsonMessage<T> {
    private Class className;
    private T payload;

    public Class getClassName() {
        return className;
    }

    public void setClassName(Class className) {
        this.className = className;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public GenericJsonMessage(Class className, T payload) {
        this.className = className;
        this.payload = payload;
    }

    public GenericJsonMessage() {
    }

    @Override
    public String toString() {
        return "GenericJsonMessage{" +
                "className=" + className +
                ", payload=" + payload +
                '}';
    }
}
