import java.util.List;

public class Book {
    private String title;
    private List<Review> reviews;

    public Book(String title, List<Review> reviews) {
        this.title = title;
        this.reviews = reviews;
    }

    public String getTitle() {
        return title;
    }

    public List<Review> getReviews() {
        return reviews;
    }

    public Double getRatingFromReviewId(String id) {
        for (Review review : this.reviews) {
            if (review.getId().equals(id)) {
                return review.getRating();
            }
        }
        return -1.0;
    }

    public String getLinkFromReviewId(String id) {
        for (Review review : this.reviews) {
            if (review.getId().equals(id)) {
                return review.getLink();
            }
        }
        return "";
    }

    @Override
    public String toString() {
        return "Book{\n" +
                "title=" + title + '\n' +
                "reviews=\n" +
                reviews +
                '}';
    }

}
