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
				Type:     schema.TypeInt,
				Optional: true,
			},
			"autotermination_minutes": {
				Type:     schema.TypeInt,
				Optional: true,
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

	if v, ok := d.GetOk("autotermination_minutes"); ok {
		request.AutoterminationMinutes = int32(v.(int))
	}

	resp, err := apiClient.Create(&request)
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
	d.Set("autotermination_minutes", resp.AutoterminationMinutes)

	return nil
}

func resourceDatabricksClusterNotExistsError(err error) bool {
	databricksError, ok := err.(client.Error)
	return ok &&
		databricksError.Code() == "INVALID_PARAMETER_VALUE" &&
		strings.Contains(databricksError.Error(), "does not exist")
}

func resourceDatabricksClusterUpdate(d *schema.ResourceData, m interface{}) error {
	// TODO: implement update

	return nil
}

func resourceDatabricksClusterDelete(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).clusters

	log.Printf("[DEBUG] Deleting cluster: %s", d.Id())

	request := models.ClustersDeleteRequest{
		ClusterId: d.Id(),
	}

	err := apiClient.Delete(&request)
	if err != nil {
		return err
	}

	d.SetId("")

	return nil
}
