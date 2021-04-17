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
                "description: " + title + '\n' +
                "reviews:\n" +
                reviews +
                '}';
    }

}
