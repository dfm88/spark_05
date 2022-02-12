package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class WordOccureces implements Serializable {
    String word;
    int occurencies;

    public WordOccureces(String word, int occurencies) {
        this.word = word;
        this.occurencies = occurencies;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getOccurencies() {
        return occurencies;
    }

    public void setOccurencies(Integer occurencies) {
        this.occurencies = occurencies;
    }

    @Override
    public String toString() {
        return "WordOccureces{" +
                "word='" + word + '\'' +
                ", occurencies=" + occurencies +
                '}';
    }
}
