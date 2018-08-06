package databricks

import (
	"github.com/betabandido/databricks-sdk-go/client"
	"github.com/betabandido/databricks-sdk-go/models"
	"github.com/hashicorp/terraform/helper/schema"
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
			"permanently_delete": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
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

	if v, ok := d.GetOk("name"); ok {
		request.ClusterName = v.(string)
	}

	if v, ok := d.GetOk("num_workers"); ok {
		request.NumWorkers = int32(v.(int))
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

	resp, err := apiClient.CreateSync(&request)
	if err != nil {
		return err
	}

	d.SetId(resp.ClusterId)

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

	d.Set("name", resp.ClusterName)
	d.Set("spark_version", resp.SparkVersion)
	d.Set("node_type_id", resp.NodeTypeId)
	d.Set("num_workers", resp.NumWorkers)
	d.Set("autoscale", resourceDatabricksClusterFlattenAutoscale(resp.Autoscale))
	d.Set("autotermination_minutes", resp.AutoterminationMinutes)
	d.Set("aws_attributes", resourceDatabricksClusterFlattenAwsAttributes(resp.AwsAttributes))

	return nil
}

func resourceDatabricksClusterUpdate(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	log.Printf("[DEBUG] Updating cluster: %s", d.Id())

	request := models.ClustersEditRequest{}

	request.ClusterId = d.Id()
	request.SparkVersion = d.Get("spark_version").(string)
	request.NodeTypeId = d.Get("node_type_id").(string)

	if v, ok := d.GetOk("num_workers"); ok {
		request.NumWorkers = int32(v.(int))
	}

	if v, ok := d.GetOk("autoscale"); ok {
		autoscale := resourceDatabricksClusterExpandAutoscale(v.(*schema.Set).List())
		request.Autoscale = &autoscale
	}

	if d.HasChange("name") {
		request.ClusterName = d.Get("name").(string)
	}

	if d.HasChange("autotermination_minutes") {
		request.AutoterminationMinutes = int32(d.Get("autotermination_minutes").(int))
	}

	if d.HasChange("aws_attributes") {
		value := d.Get("awsAttributes").(*schema.Set).List()
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(value)
		request.AwsAttributes = &awsAttributes
	}

	return apiClient.EditSync(&request)
}

func resourceDatabricksClusterDelete(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	log.Printf("[DEBUG] Deleting cluster: %s", d.Id())

	request := models.ClustersDeleteRequest{
		ClusterId: d.Id(),
	}

	err := apiClient.DeleteSync(&request)
	if err != nil {
		return err
	}

	if d.Get("permanently_delete").(bool) {
		err := apiClient.PermanentDelete(&models.ClustersPermanentDeleteRequest{
			ClusterId: d.Id(),
		})
		if err != nil {
			return err
		}
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

	if v, ok := awsAttributesElem["zone_id"]; ok {
		result.ZoneId = v.(string)
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
		if awsAttributes.EbsVolumeType != nil {
			attrs["ebs_volume_type"] = string(*awsAttributes.EbsVolumeType)
			attrs["ebs_volume_count"] = int(awsAttributes.EbsVolumeCount)
			attrs["ebs_volume_size"] = int(awsAttributes.EbsVolumeSize)
		}

		result = append(result, attrs)
	}

	return result
}
