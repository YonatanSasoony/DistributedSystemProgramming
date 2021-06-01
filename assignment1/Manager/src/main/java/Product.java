import java.util.List;

public class Product {
    private String title;
    private List<Review> reviews;

    public Product(String title, List<Review> reviews) {
        this.title = title;
        this.reviews = reviews;
    }

    public String title() {
        return title;
    }

    public List<Review> reviews() {
        return reviews;
    }

    @Override
    public String toString() {
        return "Product {\n" +
                "description: " + title + '\n' +
                "reviews:\n" +
                reviews +
                '}';
    }

}
