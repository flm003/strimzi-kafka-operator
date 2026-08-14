/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.exporter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.StrimziProbe;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of the Kafka Exporter deployment.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({
    "image", "groupRegex", "topicRegex", 
    "groupExcludeRegex", "topicExcludeRegex",
    "resources", "logging", "livenessProbe", "readinessProbe",
    "enableSaramaLogging", "showAllOffsets", "additionalConfigOptions", "template"})
@EqualsAndHashCode
@ToString
public class KafkaExporterSpec implements HasLivenessProbe, HasReadinessProbe, UnknownPropertyPreserving {
    /**
     * Allowed additional configuration options for Kafka Exporter.
     * These options will be passed as command-line flags to the kafka_exporter binary.
     * Operator-owned flags are forbidden to prevent conflicts with managed configuration.
     */
    public static final String ALLOWED_CONFIG_OPTIONS = 
        "--sasl.enabled, --sasl.handshake, --sasl.username, --sasl.password, --sasl.mechanism, --sasl.service-name, " +
        "--sasl.kerberos-config-path, --sasl.kerberos-keytab-path, --sasl.kerberos-realm, --sasl.kerberos-service-name, " +
        "--sasl.scram-client-id, --sasl.scram-client-secret, --sasl.token-provider, " +
        "--use-consumelag-zookeeper, --zookeeper.server, --kafka.labels, --refresh.metadata, --concurrent.enable, " +
        "--topic.workers, --throttle.factor, --topic.filter-out, --group.filter-out";

    private String image;
    private String groupRegex = ".*";
    private String topicRegex = ".*";
    private String topicExcludeRegex;
    private String groupExcludeRegex;

    private ResourceRequirements resources;
    private StrimziProbe livenessProbe;
    private StrimziProbe readinessProbe;
    private String logging = "info";
    private boolean enableSaramaLogging;
    private boolean showAllOffsets = true;
    private List<String> additionalConfigOptions;
    private KafkaExporterTemplate template;

    private Map<String, Object> additionalProperties;

    @Description("The container image used for the Kafka Exporter pods. "
        + "If no image name is explicitly specified, the image name corresponds to the version specified in the Cluster Operator configuration. "
        + "If an image name is not defined in the Cluster Operator configuration, a default value is used.")       
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Regular expression to specify which consumer groups to collect. " +
            "Default value is `.*`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroupRegex() {
        return groupRegex;
    }

    public void setGroupRegex(String groupRegex) {
        this.groupRegex = groupRegex;
    }

    @Description("Regular expression to specify which topics to collect. " +
            "Default value is `.*`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTopicRegex() {
        return topicRegex;
    }

    public void setTopicRegex(String topicRegex) {
        this.topicRegex = topicRegex;
    }

    @Description("Regular expression to specify which consumer groups to exclude.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroupExcludeRegex() {
        return groupExcludeRegex;
    }

    public void setGroupExcludeRegex(String groupExcludeRegex) {  
        this.groupExcludeRegex = groupExcludeRegex;
    }

    @Description("Regular expression to specify which topics to exclude.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTopicExcludeRegex() {
        return topicExcludeRegex;
    }

    public void setTopicExcludeRegex(String topicExcludeRegex) {
        this.topicExcludeRegex = topicExcludeRegex;
    }

    @Description("Enable Sarama logging, a Go client library used by the Kafka Exporter.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean getEnableSaramaLogging() {
        return enableSaramaLogging;
    }

    public void setEnableSaramaLogging(boolean enableSaramaLogging) {
        this.enableSaramaLogging = enableSaramaLogging;
    }

    @Description("Whether show the offset/lag for all consumer group, otherwise, only show connected consumer groups.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean getShowAllOffsets() {
        return showAllOffsets;
    }

    public void setShowAllOffsets(boolean showAllOffsets) {
        this.showAllOffsets = showAllOffsets;
    }

    @Description("Additional configuration options for the Kafka Exporter. " +
            "Allowed options: " + ALLOWED_CONFIG_OPTIONS + ". " +
            "These options are used to configure advanced features of Kafka Exporter (e.g., SASL authentication, concurrent processing). " +
            "Operator-owned options (such as `--kafka.server`, `--kafka.version`, `--tls.*` client auth, `--web.listen-address`, " +
            "`--group.filter`, `--topic.filter`, `--group.exclude`, `--topic.exclude`, `--verbosity`, `--log.enable-sarama`, " +
            "`--no-offset.show-all`) cannot be overridden as they are managed by the operator. " +
            "Options that are not on the allowed list will be rejected at reconcile time.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAdditionalConfigOptions() {
        return additionalConfigOptions;
    }

    public void setAdditionalConfigOptions(List<String> additionalConfigOptions) {
        this.additionalConfigOptions = additionalConfigOptions;
    }

    @Description("Only log messages with the given severity or above. " +
            "Valid levels: [`info`, `debug`, `trace`]. " +
            "Default log level is `info`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getLogging() {
        return logging;
    }

    public void setLogging(String logging) {
        this.logging = logging;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    @Description("CPU and memory resources to reserve.")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod liveness check.")
    public StrimziProbe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(StrimziProbe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod readiness check.")
    public StrimziProbe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(StrimziProbe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @Description("Customization of deployment templates and pods.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaExporterTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaExporterTemplate template) {
        this.template = template;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
