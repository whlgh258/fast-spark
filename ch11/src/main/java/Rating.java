import java.io.Serializable;

/**
 * Created by wanghl on 17-3-25.
 */
public class Rating implements Serializable {
    private int user;
    private int product;
    private double rating;

    public Rating(){

    }

    public Rating(int user, int product, double rating){
        this.user = user;
        this.product = product;
        this.rating = rating;
    }

    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public int getProduct() {
        return product;
    }

    public void setProduct(int product) {
        this.product = product;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public String toString(){
        return "[" + user + "," + product + "," + rating + "]";
    }
}
