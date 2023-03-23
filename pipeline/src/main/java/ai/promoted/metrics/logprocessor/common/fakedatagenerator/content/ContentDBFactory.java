package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

public class ContentDBFactory {

  public static ContentDB create(long platformId, ContentDBFactoryOptions options) {
    ContentDBFactory factory = new ContentDBFactory(platformId, options);
    factory.addStores();
    return factory.db;
  }

  private final ContentDBFactoryOptions options;
  private final long platformId;
  private final ContentDB db;
  private long storeId = 0;
  private long itemId = 0;
  private long promotionId = 0;

  private ContentDBFactory(long platformId, ContentDBFactoryOptions options) {
    this.options = options;
    this.platformId = platformId;
    this.db = new ContentDB();
  }

  private void addStores() {
    for (int i = 0; i < options.stores(); i++) {
      addStore();
    }
  }

  private void addStore() {
    storeId++;
    String storeIdString = String.format("s-%d-%d", platformId, storeId);
    db.addContent(
        Content.builder()
            .setContentId(storeIdString)
            .addRequestField(Content.TYPE, ContentType.STORE.name())
            // Redundantly stores the contentId so it's easier to filter.
            .addRequestField(ContentType.STORE.id, storeIdString)
            .addDocumentField("storeName", "Store " + storeIdString)
            .build());
    for (int i = 0; i < options.itemsPerStore(); i++) {
      addItem(storeIdString);
    }
  }

  private void addItem(String storeIdString) {
    itemId++;
    String itemIdString = String.format("i-%d-%d", platformId, itemId);
    db.addContent(
        Content.builder()
            .setContentId(itemIdString)
            .addRequestField(Content.TYPE, ContentType.ITEM.name())
            .addRequestField(ContentType.STORE.id, storeIdString)
            // Redundantly stores the contentId so it's easier to filter.
            .addRequestField(ContentType.ITEM.id, itemIdString)
            .addDocumentField("contentName", "Item " + itemIdString)
            .addDocumentField("storeName", "Store " + storeIdString)
            .build());
    for (int i = 0; i < options.promotionsPerItem(); i++) {
      addPromotion(storeIdString, itemIdString);
    }
  }

  private void addPromotion(String storeIdString, String itemIdString) {
    promotionId++;
    String promotionIdString = String.format("p-%d-%d", platformId, promotionId);
    db.addContent(
        Content.builder()
            .setContentId(promotionIdString)
            .addRequestField(Content.TYPE, ContentType.PROMOTION.name())
            .addRequestField(ContentType.STORE.id, storeIdString)
            .addRequestField(ContentType.ITEM.id, itemIdString)
            // Redundantly stores the contentId so it's easier to filter.
            .addRequestField(ContentType.PROMOTION.id, promotionIdString)
            .build());
  }
}
