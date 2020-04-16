package databricks

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/helper/customdiff"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/nenetto/databricks-sdk-go/client"
	"github.com/nenetto/databricks-sdk-go/models"
	"strings"
	"time"
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
		Timeouts: &schema.ResourceTimeout{
		Create: schema.DefaultTimeout(5 * time.Minute),
		},
		CustomizeDiff: customdiff.All(
			customdiff.ForceNewIfChange("arn", func (old, new, meta interface{}) bool {
				return new.(string) != old.(string)
			}),
		),
	}
}

func resourceDatabricksInstanceprofileCreate(d *schema.ResourceData, m interface{}) error {
	instanceProfileArn := d.Get("arn").(string)

	apiClient := m.(*Client).instanceprofile

	request := models.InstanceprofileAddRequest{
		InstanceProfileArn: instanceProfileArn,
	}

	err := apiClient.Add(&request)

	return resource.Retry(d.Timeout(schema.TimeoutCreate), func() *resource.RetryError {

		if err != nil {
			// If the instance is already added
			if strings.Contains(err.Error(), "has already been added") {
				d.SetId(instanceProfileArn)
				return nil

			// If the API failed, try again
			} else if strings.Contains(err.Error(), "with Body length 0"){
				return resource.RetryableError(fmt.Errorf("[TRACE] 🔴 %s", err))

			// Other case
			} else{
				d.SetId("")
				return resource.NonRetryableError(fmt.Errorf("[TRACE] 🔥 %s", err))
			}
		}

		d.SetId(instanceProfileArn)
		return nil

	})
}

func resourceDatabricksInstanceprofileDelete(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).instanceprofile

	request := models.InstanceprofileRemoveRequest{
		InstanceProfileArn: d.Get("arn").(string),
	}

	err := apiClient.Remove(&request)
	if err != nil {
		databricksError, ok := err.(client.Error)
		if ok && strings.Contains(databricksError.Error(), "has been added to") {
			d.SetId("")
			return nil
		}
		return err
	}

	d.SetId("")
	return nil
}

func resourceDatabricksInstanceprofileRead(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).instanceprofile

	request := models.InstanceprofileInfo{
		InstanceProfileArn: d.Id(),
	}

	found := apiClient.Exist(&request)

	if found {
		_ = d.Set("arn", request.InstanceProfileArn)
		_ = d.Set("is_meta_instance_profile", request.IsMetaInstanceProfile)
		return nil
	} else{
		d.SetId("")
		_ = d.Set("arn", request.InstanceProfileArn)
		_ = d.Set("is_meta_instance_profile", request.IsMetaInstanceProfile)
	}
	return nil
}

func resourceDatabricksInstanceprofileUpdate(d *schema.ResourceData, m interface{}) error {
	return resourceDatabricksInstanceprofileRead(d, m)
}
