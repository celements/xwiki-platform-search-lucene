package com.celements.search.lucene;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;

public class RandomFieldComparator extends FieldComparator<Integer> {

  private final Random random = new Random();

  @Override
  public int compare(int slot1, int slot2) {
    return random.nextInt();
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    return random.nextInt();
  }

  @Override
  public void copy(int slot, int doc) throws IOException {}

  @Override
  public void setBottom(int bottom) {}

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {}

  @Override
  public Integer value(int slot) {
    return random.nextInt();
  }

  public static class RandomFieldComparatorSource extends FieldComparatorSource {

    private static final long serialVersionUID = 8230607194271623233L;

    @Override
    public FieldComparator<Integer> newComparator(String fieldname, int numHits, int sortPos,
        boolean reversed) throws IOException {
      return new RandomFieldComparator();
    }
  }
}
