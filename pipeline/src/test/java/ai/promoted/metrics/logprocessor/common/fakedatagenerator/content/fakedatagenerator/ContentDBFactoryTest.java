package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.fakedatagenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.Content;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDB;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactoryOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFilter;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.Test;

// TODO - fix package.
public class ContentDBFactoryTest {

  @Test
  public void onePerType() {
    ContentDB db =
        ContentDBFactory.create(
            1L,
            ContentDBFactoryOptions.builder()
                .setStores(1)
                .setItemsPerStore(1)
                .setPromotionsPerItem(1)
                .build());

    List<Content> contents = db.listContent();
    assertEquals(3, contents.size());
    List<Content> stores =
        db.listContent(ContentDBFilter.whereFieldIs(Content.TYPE, ContentType.STORE.name()));
    assertEquals(1, stores.size());
    assertEquals(
        Content.builder()
            .setContentId("s-1-1")
            .setRequestFields(ImmutableMap.of("type", "STORE", "storeId", "s-1-1"))
            .setDocumentFields(ImmutableMap.of("storeName", "Store s-1-1"))
            .build(),
        stores.get(0));
    List<Content> items =
        db.listContent(ContentDBFilter.whereFieldIs(Content.TYPE, ContentType.ITEM.name()));
    assertEquals(1, items.size());
    assertEquals(
        Content.builder()
            .setContentId("i-1-1")
            .setRequestFields(
                ImmutableMap.of("type", "ITEM", "storeId", "s-1-1", "itemId", "i-1-1"))
            .setDocumentFields(
                ImmutableMap.of("contentName", "Item i-1-1", "storeName", "Store s-1-1"))
            .build(),
        items.get(0));
    List<Content> promotions =
        db.listContent(ContentDBFilter.whereFieldIs(Content.TYPE, ContentType.PROMOTION.name()));
    assertEquals(1, promotions.size());
    assertEquals(
        Content.builder()
            .setContentId("p-1-1")
            .setRequestFields(
                ImmutableMap.of(
                    "type", "PROMOTION",
                    "storeId", "s-1-1",
                    "itemId", "i-1-1",
                    "promotionId", "p-1-1"))
            .build(),
        promotions.get(0));
  }

  @Test
  public void listContent_filters() {
    ContentDB db =
        ContentDBFactory.create(
            1L,
            ContentDBFactoryOptions.builder()
                .setStores(2)
                .setItemsPerStore(2)
                .setPromotionsPerItem(2)
                .build());
    List<Content> contents = db.listContent();
    assertEquals(2 + 4 + 8, contents.size());

    assertEquals(
        1,
        db.listContent(
                ImmutableList.of(
                    ContentDBFilter.whereFieldIs(Content.TYPE, ContentType.STORE.name()),
                    ContentDBFilter.whereFieldIs(ContentType.STORE.id, "s-1-2")))
            .size());
    assertEquals(1, db.listContent(ContentDBFilter.whereContentIdIs("s-1-2")).size());
    // This finds all Stores, Items and Promotions with this ID.
    assertEquals(
        1 + 2 + 4,
        db.listContent(ContentDBFilter.whereFieldIs(ContentType.STORE.id, "s-1-2")).size());

    assertEquals(
        1,
        db.listContent(
                ImmutableList.of(
                    ContentDBFilter.whereFieldIs(Content.TYPE, ContentType.ITEM.name()),
                    ContentDBFilter.whereFieldIs(ContentType.ITEM.id, "i-1-4")))
            .size());
    assertEquals(1, db.listContent(ContentDBFilter.whereContentIdIs("i-1-4")).size());
    // This finds all Items and Promotions with those IDs.
    assertEquals(
        1 + 2, db.listContent(ContentDBFilter.whereFieldIs(ContentType.ITEM.id, "i-1-4")).size());

    assertEquals(
        1,
        db.listContent(
                ImmutableList.of(
                    ContentDBFilter.whereFieldIs(Content.TYPE, ContentType.PROMOTION.name()),
                    ContentDBFilter.whereFieldIs(ContentType.PROMOTION.id, "p-1-8")))
            .size());
    assertEquals(1, db.listContent(ContentDBFilter.whereContentIdIs("p-1-8")).size());
    // Just finds the one Promotion.
    assertEquals(
        1, db.listContent(ContentDBFilter.whereFieldIs(ContentType.PROMOTION.id, "p-1-8")).size());
  }

  @Test
  public void getContent() {
    ContentDB db =
        ContentDBFactory.create(
            1L,
            ContentDBFactoryOptions.builder()
                .setStores(2)
                .setItemsPerStore(2)
                .setPromotionsPerItem(2)
                .build());

    assertEquals("s-1-2", db.getContent("s-1-2").contentId());
    assertEquals("i-1-4", db.getContent("i-1-4").contentId());
    assertEquals("p-1-8", db.getContent("p-1-8").contentId());
  }
}
