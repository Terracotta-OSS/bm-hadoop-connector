package org.terracotta.bigmemory.hadoop;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

/**
 * Adds Clustered Ehcache specific configuration to Hadoop Configuration
 *
 */
public class BigmemoryHadoopConfiguration extends Configuration {

	public static final String EHCACHE_CONFIG_XML = "ehcache.xml";
	public static final String BM_OUTPUT_CACHE = "bigmemory.output.cache";
	public static final String BM_BULK_LOAD = "bigmemory.set.bulkload";
	public static final String BM_THREADS = "bigmemory.unboundedCommit.threads";
	public static final String BM_COMMIT_THREADS = "bigmemory.commit.threads";

  public static Configuration addEhcacheResources(Configuration conf) {
	  conf.addResource(EHCACHE_CONFIG_XML);
    return conf;
  }

  /**
   * Creates a Configuration with Ehcache Config
   * @return a Configuration with Ehcache resources
   */
  public static Configuration create() {
    Configuration conf = new Configuration();
    return addEhcacheResources(conf);
  }

  /**
   * Creates a clone of passed configuration.
   * @param that Configuration to clone.
   * @return a Configuration created with the hbase-*.xml files plus
   * the given configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

  /**
   * Merge two configurations.
   * @param destConf the configuration that will be overwritten with items
   *                 from the srcConf
   * @param srcConf the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }

}
