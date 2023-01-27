package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

public enum ContentType {
    /**
     * Items in a store.
     */
    ITEM("itemId"),
    /**
     * An entity that contains multiple items.
     */
    STORE("storeId"),
    /**
     * A special promotion for an item.  There can be multiple Promotions for Items.
     */
    PROMOTION("promotionId");

    public final String id;

    ContentType(String id) {
        this.id = id;
    }
}
