package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.auto.value.AutoValue;

/** Options for ContentDBFactory. */
@AutoValue
public abstract class ContentDBFactoryOptions {
    abstract int stores();
    abstract int itemsPerStore();
    abstract int promotionsPerItem();

    public static Builder builder() {
        return new AutoValue_ContentDBFactoryOptions.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setStores(int stores);
        public abstract Builder setItemsPerStore(int itemsPerStore);
        public abstract Builder setPromotionsPerItem(int promotionsPerItem);
        public abstract ContentDBFactoryOptions build();
    }
}
