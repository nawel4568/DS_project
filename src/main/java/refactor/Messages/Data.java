package refactor.Messages;

import java.util.Objects;

public class Data {
    public final Integer value;
    private boolean stable;

    public static Data defaultData(){
        return new Data(Integer.MIN_VALUE, true);
    }

    public Data(Integer value, boolean stable) {
        this.value = value;
        this.stable = stable;
    }
    public Data(Data other){
        this.value = other.value;;
        this.stable = other.stable;
    }

    public void setStable(boolean stable) {
        this.stable = stable;
    }
    public boolean isStable(){ return this.stable; }

    @Override
    public int hashCode() {
        return Objects.hash(value, stable);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Data other = (Data) obj;
        return Objects.equals(this.value, other.value) && this.stable == other.stable;
    }

    @Override
    public String toString() {
        return "Data{" +
                "value=" + value +
                ", stable=" + stable +
                '}';
    }

}
