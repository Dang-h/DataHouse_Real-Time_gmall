package utils;

public class RanOpt<T>{
    T value ;
    int weight;

    //TODO ??作用
    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
