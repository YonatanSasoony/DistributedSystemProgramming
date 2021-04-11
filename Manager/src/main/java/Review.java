public class Review {
    private String id;
    private String link;
    private String title;
    private String text;
    private Double rating;
    private String author;
    private String date;

    public Review(String id, String link, String title, String text, Double rating, String author, String date) {
        this.id = id;
        this.link = link;
        this.title = title;
        this.text = text;
        this.rating = rating;
        this.author = author;
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public String getLink() {
        return link;
    }

    public String getTitle() {
        return title;
    }

    public String getText() {
        return text;
    }

    public Double getRating() {
        return rating;
    }

    public String getAuthor() {
        return author;
    }

    public String getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "Review{\n" +
                "id='" + id + '\'' + '\n' +
                ", link='" + link + '\'' + '\n' +
                ", title='" + title + '\'' + '\n' +
                ", text='" + text + '\'' + '\n' +
                ", rating=" + rating + '\n' +
                ", author='" + author + '\'' + '\n' +
                ", date='" + date + '\'' + '\n' +
                '}' + '\n';
    }
}
