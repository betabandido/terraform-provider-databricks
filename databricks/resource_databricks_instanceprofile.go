package databricks

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/nenetto/databricks-sdk-go/models"
	"log"
)

func resourceDatabricksInstanceprofile() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabricksInstanceprofileCreate,
		Read:   resourceDatabricksInstanceprofileRead,
		Update: resourceDatabricksInstanceprofileUpdate,
		Delete: resourceDatabricksInstanceprofileDelete,

		Schema: map[string]*schema.Schema{
			"arn": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}

func resourceDatabricksInstanceprofileCreate(d *schema.ResourceData, m interface{}) error {
	log.Print("[DEBUG] Creating Databricks Instance Profile")
	instanceProfileArn := d.Get("arn").(string)

	apiClient := m.(*Client).instanceprofile

	request := models.InstanceprofileAddRequest{
		InstanceProfileArn: instanceProfileArn,
	}

	err := apiClient.Add(&request)
	if err != nil {
		log.Print(err)
		return err
	}

	d.SetId(instanceProfileArn)

	log.Printf("[DEBUG] Instance Profile ID: %s", d.Id())
	return resourceDatabricksInstanceprofileRead(d, m)
}

func resourceDatabricksInstanceprofileDelete(d *schema.ResourceData, m interface{}) error {
	log.Printf("[DEBUG] Deleting cluster: %s", d.Id())
	apiClient := m.(*Client).instanceprofile

	request := models.InstanceprofileRemoveRequest{
		InstanceProfileArn: d.Get("arn").(string),
	}

	err := apiClient.Remove(&request)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func resourceDatabricksInstanceprofileRead(d *schema.ResourceData, m interface{}) error {
	log.Printf("[DEBUG] Reading Instance Profiles: %s", d.Id())

	apiClient := m.(*Client).instanceprofile

	resp, err := apiClient.List()
	if err == nil {
		for _, ip := range resp.InstanceProfiles {
			if ip.InstanceProfileArn == d.Id() {
				d.SetId(ip.InstanceProfileArn)
				return nil
			}
		}
		log.Printf("[WARN] Instance Profile (%s) not registered, removing from state", d.Id())
		d.SetId("")
		return nil
	}
	d.SetId("")
	return nil
}

func resourceDatabricksInstanceprofileUpdate(d *schema.ResourceData, m interface{}) error {

	if d.HasChange("arn") {
		// Delete and recreate
		err := resourceDatabricksClusterDelete(d,m)
		if err!=nil{
			return err
		}
		return resourceDatabricksInstanceprofileCreate(d, m)
	}

	return resourceDatabricksInstanceprofileRead(d, m)
}
