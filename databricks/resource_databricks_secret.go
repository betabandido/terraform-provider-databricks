package databricks

import (
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/tcz001/databricks-sdk-go/models"
)

func resourceDatabricksSecret() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabricksSecretPut,
		Update: resourceDatabricksSecretPut,
		Read:   resourceDatabricksSecretRead,
		Delete: resourceDatabricksSecretDelete,

		Schema: map[string]*schema.Schema{
			"key": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"scope": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"string_value": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"bytes_value": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceDatabricksSecretPut(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).secrets

	log.Print("[DEBUG] Creating secret")

	request := models.SecretsPutRequest{
		Scope: d.Get("scope").(string),
		Key:   d.Get("key").(string),
	}

	if v, ok := d.GetOk("string_value"); ok {
		request.StringValue = v.(string)
	}

	if v, ok := d.GetOk("byte_value"); ok {
		request.BytesValue = v.(string)
	}

	err := apiClient.Put(&request)
	if err != nil {
		return err
	}

	d.SetId(d.Get("scope").(string) + "/" + d.Get("key").(string))

	log.Printf("[DEBUG] Secret Name: %s", d.Id())

	return nil
}

func resourceDatabricksSecretRead(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).secrets

	request := models.SecretsListRequest{
		Scope: d.Get("scope").(string),
	}

	resp, err := apiClient.List(&request)
	if err != nil {
		return err
	}
	if !resourceDatabricksSecretNotExistsError(d.Get("key").(string), resp) {
		log.Printf("[WARN] Secret (%s) not found, removing from state", d.Id())
		d.SetId("")
		return nil
	}

	d.SetId(d.Get("scope").(string) + "/" + d.Get("key").(string))

	return nil
}

func resourceDatabricksSecretDelete(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).secrets

	log.Printf("[DEBUG] Deleting secret: %s", d.Id())

	request := models.SecretsDeleteRequest{
		Scope: d.Get("scope").(string),
		Key:   d.Get("key").(string),
	}

	err := apiClient.Delete(&request)
	if err != nil {
		return err
	}

	d.SetId("")

	return nil
}

func resourceDatabricksSecretNotExistsError(key string, resp *models.SecretsListResponse) bool {
	scopes := resp.Secrets
	for _, scope := range scopes {
		if scope.Key == key {
			return true
		}
	}
	return false
}
