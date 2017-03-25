import java.io.Serializable;

/**
 * Created by wanghl on 17-3-25.
 */
public class Number implements Serializable {
    private Integer number;

    public Number(){

    }

    public Number(Integer number){
        this.number = number;
    }

    public Integer getNumber(){
        return this.number;
    }

    public void setNumber(Integer number){
        this.number = number;
    }
}
