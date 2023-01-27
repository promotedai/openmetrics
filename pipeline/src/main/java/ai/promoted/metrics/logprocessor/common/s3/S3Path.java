package ai.promoted.metrics.logprocessor.common.s3;

import com.google.auto.value.AutoValue;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility containing useful constants.
 **/
@AutoValue
public abstract class S3Path {
    private static final String SEPARATOR = "/";
    // The string from the protocol to top level domain.
    public abstract String root();

    /** Optional label to prefix before the subDirs.  Used to create a separate set of outputs for non-live join jobs.
     * Empty string means not set.
     */
    public abstract Optional<String> joinLabel();
    public abstract ImmutableList<String> subDirs();

    public abstract Builder toBuilder();

    public static S3Path.Builder builder() {
        return new AutoValue_S3Path.Builder()
                .setRoot("");
    }

    public ImmutableList<String> allPathParts() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (!joinLabel().orElse("").isEmpty()) {
            builder.add(joinLabel().get());
        }
        builder.addAll(subDirs());
        return builder.build();
    }

    // TODO - move it.
    public String toString() {
        String root = root();
        if (!root.endsWith(SEPARATOR)) {
            root += SEPARATOR;
        }
        return root + allPathParts().stream().map(s -> verifyNoSeparator(s) + SEPARATOR).collect(Collectors.joining());
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setRoot(String flagOverride);
        public abstract Builder setJoinLabel(String joinLabel);
        public abstract Builder setJoinLabel(Optional<String> joinLabel);
        public abstract Builder setSubDirs(Iterable<String> subDirs);
        public abstract Builder setSubDirs(String... subDirs);
        abstract ImmutableList.Builder<String> subDirsBuilder();
        public final Builder addSubDir(String subDir) {
            subDirsBuilder().add(subDir);
            return this;
        }
        public final Builder addSubDirs(String... subDirs) {
            subDirsBuilder().add(subDirs);
            return this;
        }
        public final Builder addSubDirs(Iterable<String> subDirs) {
            subDirsBuilder().addAll(subDirs);
            return this;
        }
        public Builder prependSubdirs(String... prefixDirs) {
            S3Path tmp = build();
            return tmp.toBuilder().setSubDirs(FluentIterable.from(prefixDirs).append(tmp.subDirs()).toList());
        }
        public Builder clone() {
            return build().toBuilder();
        }
        public abstract S3Path build();
    }

    static String verifyNoSeparator(String pathPart) {
        if (pathPart.contains(SEPARATOR)) {
            throw new IllegalArgumentException("S3 path part should not contain a path separator, pathPart=" + pathPart);
        }
        return pathPart;
    }
}
