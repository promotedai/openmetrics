package org.apache.flink.connector.file.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public interface SuffixedFileSystemConnectorOptions {
  ConfigOption<String> SUFFIX_NAME =
      ConfigOptions.key("suffix")
          .stringType()
          .defaultValue("")
          .withDescription("The suffix to add to the end of files (e.g. `.csv`).");
}
