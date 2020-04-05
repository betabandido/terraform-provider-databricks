package databricks

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"domain": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"token": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"databricks_cluster":  resourceDatabricksCluster(),
			"databricks_notebook": resourceDatabricksNotebook(),
		},
		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	config := Config{}

	if domain, ok := d.GetOk("domain"); ok {
		s := domain.(string)
		config.Domain = &s
	}

	if token, ok := d.GetOk("token"); ok {
		s := token.(string)
		config.Token = &s
	}

	return config.Client()
}