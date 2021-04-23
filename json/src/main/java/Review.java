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

    public String id() {
        return id;
    }

    public String link() {
        return link;
    }

    public String title() {
        return title;
    }

    public String text() {
        return text;
    }

    public Double rating() {
        return rating;
    }

    public String author() {
        return author;
    }

    public String date() {
        return date;
    }

    @Override
    public String toString() {
        return "Review{\n" +
                "id: " + id + '\n' +
                "link: " + link + '\n' +
                "title: " + title + '\n' +
                "text: " + text + '\n' +
                "rating: " + rating + '\n' +
                "author: " + author + '\n' +
                "date: " + date + '\n' +
                '}' + '\n';
    }
}
