import java.util.List;

public class Product {
    private String description;
    private List<Review> reviews;

    public Product(String description, List<Review> reviews) {
        this.description = description;
        this.reviews = reviews;
    }

    public String description() {
        return description;
    }

    public List<Review> reviews() {
        return reviews;
    }

    public Double ratingFromReviewId(String id) {
        for (Review review : this.reviews) {
            if (review.id().equals(id)) {
                return review.rating();
            }
        }
        return -1.0;
    }

    public String linkFromReviewId(String id) {
        for (Review review : this.reviews) {
            if (review.id().equals(id)) {
                return review.link();
            }
        }
        return "";
    }

    @Override
    public String toString() {
        return "Product {\n" +
                "description: " + description + '\n' +
                "reviews:\n" +
                reviews +
                '}';
    }

}
