package databricks

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/nenetto/databricks-sdk-go/client"
	"github.com/nenetto/databricks-sdk-go/models"
	"log"
	"strings"
)

func resourceDatabricksCluster() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabricksClusterCreate,
		Read:   resourceDatabricksClusterRead,
		Update: resourceDatabricksClusterUpdate,
		Delete: resourceDatabricksClusterDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"spark_version": {
				Type:     schema.TypeString,
				Required: true,
			},
			"node_type_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"num_workers": {
				Type:          schema.TypeInt,
				Optional:      true,
				ConflictsWith: []string{"autoscale"},
			},
			"autoscale": {
				Type:     schema.TypeSet,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"min_workers": {
							Type:     schema.TypeInt,
							Required: true,
						},
						"max_workers": {
							Type:     schema.TypeInt,
							Required: true,
						},
					},
				},
				ConflictsWith: []string{"num_workers"},
			},
			"spark_conf": {
				Type:     schema.TypeMap,
				Optional: true,
			},
			"spark_env_vars": {
				Type:     schema.TypeMap,
				Optional: true,
			},
			"custom_tags": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     schema.TypeString,
			},
			"autotermination_minutes": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"aws_attributes": {
				Type:     schema.TypeSet,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"zone_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"instance_profile_arn": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"ebs_volume_type": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"ebs_volume_count": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"ebs_volume_size": {
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},
			"cluster_log_conf": {
				Type:     schema.TypeSet,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"s3": {
							Type:     schema.TypeSet,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"destination" : {
										Type:     schema.TypeString,
										Optional: true,
									},
									"region" : {
										Type:     schema.TypeString,
										Optional: true,
									},
									"endpoint" : {
										Type:     schema.TypeString,
										Optional: true,
									},
									"enable_encryption" : {
										Type:     schema.TypeBool,
										Optional: true,
									},
									"encryption_type" : {
										Type:     schema.TypeString,
										Optional: true,
									},
									"kms_key" : {
										Type:     schema.TypeString,
										Optional: true,
									},
									"canned_acl" : {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
						"dbfs": {
							Type:     schema.TypeSet,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"destination" : {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceDatabricksClusterCreate(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	log.Print("[DEBUG] Creating cluster")

	request := models.ClustersCreateRequest{
		SparkVersion: d.Get("spark_version").(string),
		NodeTypeId:   d.Get("node_type_id").(string),
	}

	if v, ok := d.GetOk("autoscale"); ok {
		autoscale := resourceDatabricksClusterExpandAutoscale(v.(*schema.Set).List())
		request.Autoscale = &autoscale
	}

	if v, ok := d.GetOk("autotermination_minutes"); ok {
		request.AutoterminationMinutes = int32(v.(int))
	}

	if v, ok := d.GetOk("aws_attributes"); ok {
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(v.(*schema.Set).List())
		request.AwsAttributes = &awsAttributes
	}

	if v, ok := d.GetOk("cluster_log_conf"); ok {
		clusterLogConf := resourceDatabricksClusterExpandLogConfiguration(v.(*schema.Set).List())
		request.ClusterLogConf = &clusterLogConf
	}

	if v, ok := d.GetOk("name"); ok {
		request.ClusterName = v.(string)
	}

	if v, ok := d.GetOk("custom_tags"); ok {
		customTags := map[string]string{}
		for k, v := range v.(map[string]interface{}){
			customTags[k] = v.(string)
		}
		request.CustomTags = customTags
	}

	if v, ok := d.GetOk("enable_elastic_disk"); ok {
		request.EnableElasticDisk = v.(bool)
	}

	if v, ok := d.GetOk("num_workers"); ok {
		request.NumWorkers = int32(v.(int))
	}

	if v, ok := d.GetOk("spark_conf"); ok {
		sparkConf := map[string]string{}
		for k, v := range v.(map[string]interface{}){
			sparkConf[k] = v.(string)
		}
		request.SparkConf = sparkConf
	}

	if v, ok := d.GetOk("spark_env_vars"); ok {
		sparkEnvVars := map[string]string{}
		for k, v := range v.(map[string]interface{}){
			sparkEnvVars[k] = v.(string)
		}
		request.SparkEnvVars = sparkEnvVars
	}

	// request.SshPublicKeys TODO: Not supported

	respCreate, errCreate := apiClient.CreateSync(&request)
	if errCreate != nil {
		return errCreate
	}

	// Pin and Stop the cluster which means stop it in API to preserve the cluster and avoid permanent deletion
	requestPin := models.ClusterPinRequest{
		ClusterId: respCreate.ClusterId,
	}

	errPin := apiClient.Pin(&requestPin)
	if errPin != nil {
		return errPin
	}

	requestStop := models.ClustersDeleteRequest{
		ClusterId: respCreate.ClusterId,
	}

	errStop := apiClient.DeleteSync(&requestStop)
	if errStop != nil {
		return errStop
	}

	d.SetId(respCreate.ClusterId)

	errRead := resourceDatabricksClusterRead(d,m)
	if errRead != nil {
		return errRead
	}

	log.Printf("[DEBUG] Cluster ID: %s", d.Id())

	return nil
}

func resourceDatabricksClusterRead(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	request := models.ClustersGetRequest{
		ClusterId: d.Id(),
	}

	resp, err := apiClient.Get(&request)
	if err != nil {
		if resourceDatabricksClusterNotExistsError(err) {
			log.Printf("[WARN] Cluster (%s) not found, removing from state", d.Id())
			d.SetId("")
			return nil
		}
		return err
	}

	_ = d.Set("node_type_id", resp.NodeTypeId)
	_ = d.Set("spark_version", resp.SparkVersion)
	_ = d.Set("autoscale", resourceDatabricksClusterFlattenAutoscale(resp.Autoscale))
	_ = d.Set("autotermination_minutes", resp.AutoterminationMinutes)
	_ = d.Set("aws_attributes", resourceDatabricksClusterFlattenAwsAttributes(resp.AwsAttributes))
	_ = d.Set("cluster_log_conf", resourceDatabricksClusterFlattenLogConfiguration(resp.ClusterLogConf))
	_ = d.Set("name", resp.ClusterName)
	_ = d.Set("custom_tags", resp.CustomTags)
	_ = d.Set("enable_elastic_disk", resp.EnableElasticDisk)
	_ = d.Set("spark_conf", resp.EnableElasticDisk)
	_ = d.Set("num_workers", resp.NumWorkers)
	_ = d.Set("spark_env_vars", resp.SparkEnvVars)
	_ = d.Set("cluster_memory_mb", resp.ClusterMemoryMb)
	_ = d.Set("jdbc_port", resp.JdbcPort)
	_ = d.Set("cluster_cores", resp.ClusterCores)
	_ = d.Set("default_tags", resp.DefaultTags)

	return nil
}

func resourceDatabricksClusterUpdate(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	log.Printf("[DEBUG] Updating cluster: %s", d.Id())

	request := models.ClustersEditRequest{}

	request.ClusterId = d.Id()
	request.SparkVersion = d.Get("spark_version").(string)
	request.NodeTypeId = d.Get("node_type_id").(string)

	if v, ok := d.GetOk("autoscale"); ok{
		autoscale := resourceDatabricksClusterExpandAutoscale(v.(*schema.Set).List())
		request.Autoscale = &autoscale
	}

	if v, ok := d.GetOk("num_workers"); ok {
		request.NumWorkers = int32(v.(int))
	}

	if v, ok := d.GetOk("autotermination_minutes"); ok {
		request.AutoterminationMinutes = int32(v.(int))
	}

	if v, ok := d.GetOk("aws_attributes"); ok{
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(v.(*schema.Set).List())
		request.AwsAttributes = &awsAttributes
	}

	if v, ok := d.GetOk("cluster_log_conf"); ok{
		clusterLogConf := resourceDatabricksClusterExpandLogConfiguration(v.(*schema.Set).List())
		request.ClusterLogConf = &clusterLogConf
	}

	if v, ok := d.GetOk("name"); ok {
		request.ClusterName = v.(string)
	}

	if v, ok := d.GetOk("custom_tags"); ok{
		customTags := map[string]string{}
		for k, vv := range v.(map[string]interface{}){
			customTags[k] = vv.(string)
		}
		request.CustomTags = customTags
	}

	if v, ok := d.GetOk("enable_elastic_disk"); ok {
		request.EnableElasticDisk = v.(bool)
	}

	if v, ok := d.GetOk("spark_conf"); ok{
		sparkConf := map[string]string{}
		for k, vv := range v.(map[string]interface{}){
			sparkConf[k] = vv.(string)
		}
		request.SparkConf = sparkConf
	}

	if v, ok := d.GetOk("spark_env_vars"); ok{
		sparkEnvVars := map[string]string{}
		for k, vv := range v.(map[string]interface{}){
			sparkEnvVars[k] = vv.(string)
		}
		request.SparkEnvVars = sparkEnvVars
	}

	err := apiClient.EditSync(&request)
	if err != nil {
		return err
	}

	return resourceDatabricksClusterRead(d, m)
}

func resourceDatabricksClusterDelete(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	log.Printf("[DEBUG] Deleting cluster: %s", d.Id())

	// UnPin
	requestPin := models.ClusterUnpinRequest{
		ClusterId: d.Id(),
	}

	errUnpin := apiClient.Unpin(&requestPin)
	if errUnpin != nil {
		return errUnpin
	}

	request := models.ClustersDeleteRequest{
		ClusterId: d.Id(),
	}

	errDelete := apiClient.DeleteSync(&request)
	if errDelete != nil {
		return errDelete
	}

	errPermanentDelete := apiClient.PermanentDelete(&models.ClustersPermanentDeleteRequest{
		ClusterId: d.Id(),
	})
	if errPermanentDelete != nil {
		return errPermanentDelete
	}

	d.SetId("")

	return nil
}

func resourceDatabricksClusterNotExistsError(err error) bool {
	databricksError, ok := err.(client.Error)
	return ok &&
		databricksError.Code() == "INVALID_PARAMETER_VALUE" &&
		strings.Contains(databricksError.Error(), "does not exist")
}

func resourceDatabricksClusterExpandAutoscale(autoscale []interface{}) models.ClustersAutoScale {
	autoscaleElem := autoscale[0].(map[string]interface{})

	return models.ClustersAutoScale{
		MinWorkers: int32(autoscaleElem["min_workers"].(int)),
		MaxWorkers: int32(autoscaleElem["max_workers"].(int)),
	}
}

func resourceDatabricksClusterFlattenAutoscale(autoscale *models.ClustersAutoScale) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	if autoscale != nil {
		result = append(result, map[string]interface{}{
			"min_workers": autoscale.MinWorkers,
			"max_workers": autoscale.MaxWorkers,
		})
	}
	return result
}

func resourceDatabricksClusterExpandAwsAttributes(awsAttributes []interface{}) models.ClustersAwsAttributes {
	awsAttributesElem := awsAttributes[0].(map[string]interface{})

	result := models.ClustersAwsAttributes{}

	if v, ok := awsAttributesElem["first_on_demand"]; ok {
		result.FirstOnDemand = v.(int32)
	}

	if v, ok := awsAttributesElem["availability"]; ok {
		availability := models.ClustersAwsAvailability(v.(string))
		result.Availability = &availability
	}

	if v, ok := awsAttributesElem["zone_id"]; ok {
		result.ZoneId = v.(string)
	}

	if v, ok := awsAttributesElem["instance_profile_arn"]; ok {
		result.InstanceProfileArn = v.(string)
	}

	if v, ok := awsAttributesElem["spot_bid_price_percent"]; ok {
		result.SpotBidPricePercent = v.(int32)
	}

	if v, ok := awsAttributesElem["ebs_volume_type"]; ok {
		volumeType := models.ClustersEbsVolumeType(v.(string))
		result.EbsVolumeType = &volumeType
	}

	if v, ok := awsAttributesElem["ebs_volume_count"]; ok {
		result.EbsVolumeCount = int32(v.(int))
	}

	if v, ok := awsAttributesElem["ebs_volume_size"]; ok {
		result.EbsVolumeSize = int32(v.(int))
	}

	return result
}

func resourceDatabricksClusterFlattenAwsAttributes(awsAttributes *models.ClustersAwsAttributes) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	if awsAttributes != nil {
		attrs := make(map[string]interface{})
		attrs["zone_id"] = awsAttributes.ZoneId
		attrs["instance_profile_arn"] = awsAttributes.InstanceProfileArn
		if awsAttributes.EbsVolumeType != nil {
			attrs["ebs_volume_type"] = string(*awsAttributes.EbsVolumeType)
			attrs["ebs_volume_count"] = int(awsAttributes.EbsVolumeCount)
			attrs["ebs_volume_size"] = int(awsAttributes.EbsVolumeSize)
		}

		result = append(result, attrs)
	}

	return result
}

func resourceDatabricksClusterExpandLogConfiguration(logconf []interface{}) models.ClustersClusterLogConf{

	clusterlogConfElem := logconf[0].(map[string]interface{})

	result := models.ClustersClusterLogConf{}

	if v, ok := clusterlogConfElem["s3"]; ok {
		s3confd := resourceDatabricksClusterExpandLogConfigurationS3(v.(*schema.Set).List())
		result.S3 = &s3confd
	}

	if v, ok := clusterlogConfElem["dbfs"]; ok {
		dbfsconfd := resourceDatabricksClusterExpandLogConfigurationDBFS(v.(*schema.Set).List())
		result.Dbfs = &dbfsconfd
	}

	return result
}

func resourceDatabricksClusterExpandLogConfigurationS3(s3conf []interface{}) models.ClustersS3StorageInfo {
	result := models.ClustersS3StorageInfo{}

	if len(s3conf) > 0 {
		clusterlogConfS3Elem := s3conf[0].(map[string]interface{})

		if v, ok := clusterlogConfS3Elem["destination"]; ok {
			result.Destination = v.(string)
		}

		if v, ok := clusterlogConfS3Elem["region"]; ok {
			result.Region = v.(string)
		}

		if v, ok := clusterlogConfS3Elem["endpoint"]; ok {
			result.Endpoint = v.(string)
		}

		if v, ok := clusterlogConfS3Elem["enable_encryption"]; ok {
			result.EnableEncryption = v.(bool)
		}

		if v, ok := clusterlogConfS3Elem["encryption_type"]; ok {
			encryptionType := models.ClustersLogS3EncryptionType(v.(string))
			result.EncryptionType = &encryptionType
		}

		if v, ok := clusterlogConfS3Elem["kms_key"]; ok {
			result.KmsKey = v.(string)
		}

		if v, ok := clusterlogConfS3Elem["canned_acl"]; ok {
			result.CannedACL = v.(string)
		}
	}

	return result
}

func resourceDatabricksClusterExpandLogConfigurationDBFS(dbfsconf []interface{}) models.ClustersDbfsStorageInfo {
	result := models.ClustersDbfsStorageInfo{}

	if len(dbfsconf) > 0 {
		clusterlogConfDbfsElem := dbfsconf[0].(map[string]interface{})

		if v, ok := clusterlogConfDbfsElem["destination"]; ok {
			result.Destination = v.(string)
		}
	}

	return result
}

func resourceDatabricksClusterFlattenLogConfiguration(logconf *models.ClustersClusterLogConf) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	if logconf != nil {
		attrs := make(map[string]interface{})

		if logconf.S3 != nil {
			attrs["destination"] = logconf.S3.Destination
			attrs["region"] = logconf.S3.Region
			attrs["endpoint"] = logconf.S3.Endpoint
			attrs["enable_encryption"] = logconf.S3.EnableEncryption
			if logconf.S3.EncryptionType != nil { attrs["encryption_type"] = string(*logconf.S3.EncryptionType)}
			attrs["kms_key"] = logconf.S3.KmsKey
			attrs["canned_acl"] = logconf.S3.CannedACL
		}

		if logconf.Dbfs != nil {
			attrs["destination"] = logconf.Dbfs.Destination
		}

		result = append(result, attrs)
	}

	return result
}
