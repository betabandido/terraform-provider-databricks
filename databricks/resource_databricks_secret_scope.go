package databricks

import (
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/tcz001/databricks-sdk-go/models"
)

func resourceDatabricksSecretScope() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabricksSecretScopeCreate,
		Read:   resourceDatabricksSecretScopeRead,
		Delete: resourceDatabricksSecretScopeDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"initial_manage_principal": {
				Type:     schema.TypeString,
				Default:  "users",
				Optional: true,
				ForceNew: true,
			},
		},
	}
}

func resourceDatabricksSecretScopeCreate(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).secrets

	log.Print("[DEBUG] Creating secretScope")

	request := models.SecretsScopesCreateRequest{
		Scope:                  d.Get("name").(string),
		InitialManagePrincipal: d.Get("initial_manage_principal").(string),
	}

	if v, ok := d.GetOk("name"); ok {
		request.Scope = v.(string)
	}

	err := apiClient.AddScope(&request)
	if err != nil {
		return err
	}

	d.SetId(d.Get("name").(string))

	log.Printf("[DEBUG] SecretScope Name: %s", d.Id())

	return nil
}

func resourceDatabricksSecretScopeRead(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).secrets

	resp, err := apiClient.ListScopes()
	if err != nil {
		if resourceDatabricksSecretScopeNotExistsError(d.Id(), resp) {
			log.Printf("[WARN] SecretScope (%s) not found, removing from state", d.Id())
			d.SetId("")
			return nil
		}
		return err
	}

	d.Set("name", d.Id())

	return nil
}

func resourceDatabricksSecretScopeDelete(d *schema.ResourceData, m interface{}) error {
	apiClient := m.(*Client).secrets

	log.Printf("[DEBUG] Deleting secretScope: %s", d.Id())

	request := models.SecretsScopesDeleteRequest{
		Scope: d.Id(),
	}

	err := apiClient.DeleteScope(&request)
	if err != nil {
		return err
	}

	d.SetId("")

	return nil
}

func resourceDatabricksSecretScopeNotExistsError(name string, resp *models.SecretsScopesListResponse) bool {
	scopes := resp.Scopes
	for _, scope := range scopes {
		if scope.Name == name {
			return true
		}
	}
	return false
}
