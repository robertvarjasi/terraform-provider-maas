package maas

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/maas/gomaasclient/client"
	"github.com/maas/gomaasclient/entity"
)

func resourceMaasRaid() *schema.Resource {
	return &schema.Resource{
		Description:   "Provides a resource to manage MAAS Raids.",
		CreateContext: resourceRaidCreate,
		ReadContext:   resourceRaidRead,
		UpdateContext: resourceRaidUpdate,
		DeleteContext: resourceRaidDelete,
		Importer: &schema.ResourceImporter{
			StateContext: func(ctx context.Context, d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
				idParts := strings.Split(d.Id(), ":")
				if len(idParts) != 2 || idParts[0] == "" || idParts[1] == "" {
					return nil, fmt.Errorf("unexpected format of ID (%q), expected MACHINE:RAID", d.Id())
				}
				client := m.(*client.Client)
				machine, err := getMachine(client, idParts[0])
				if err != nil {
					return nil, err
				}
				raid, err := getRaid(client, machine.SystemID, idParts[1])
				if err != nil {
					return nil, err
				}
				tfState := map[string]interface{}{
					"id":      fmt.Sprintf("%v", raid.ID),
					"machine": machine.SystemID,
					"name":    raid.Name,
					"uuid":    raid.UUID,
				}
				if err := setTerraformState(d, tfState); err != nil {
					return nil, err
				}
				return []*schema.ResourceData{d}, nil
			},
		},

		Schema: map[string]*schema.Schema{
			"machine": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The identifier (name or ID) of the machine for the new Raid.",
			},
			"id": {
				Type:        schema.TypeString,
				Required:    false,
				Optional:    true,
				Computed:    true,
				Description: "The  ID for the new Raid.",
			},
			"level": {
				Type:        schema.TypeString,
				Required:    true,
				Optional:    false,
				Computed:    false,
				Description: "The Raid level/type",
			},
			"block_devices": {
				Type:        schema.TypeList,
				Optional:    true,
				Computed:    false,
				Description: "Block devices to add to the RAID",
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"name": {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The name of the new Raid. This argument is computed if it's not set.",
			},
			"partitions": {
				Type:        schema.TypeList,
				Required:    true,
				Computed:    false,
				Description: "Partitions to add to the RAID",
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"spare_partitions": {
				Type:        schema.TypeList,
				Required:    false,
				Computed:    false,
				Optional:    true,
				Description: "Spare Partitions to add to the RAID",
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"spare_devices": {
				Type:        schema.TypeList,
				Required:    false,
				Optional:    true,
				Computed:    false,
				Description: "Spare devices to add to the RAID",
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"uuid": {
				Type:        schema.TypeString,
				Required:    false,
				Computed:    true,
				Description: "UUID of the RAID",
			},
		},
	}
}

func resourceRaidCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*client.Client)

	machine, err := getMachine(client, d.Get("machine").(string))
	if err != nil {
		return diag.FromErr(err)
	}
	raid, err := client.RAIDs.Create(machine.SystemID, getRaidsParams(d))
	if err != nil {
		return diag.FromErr(err)
	}
	d.SetId(fmt.Sprintf("%v", raid.ID))

	return resourceRaidRead(ctx, d, m)
}

func resourceRaidRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*client.Client)

	machine, err := getMachine(client, d.Get("machine").(string))
	if err != nil {
		return diag.FromErr(err)
	}
	raid, err := getRaid(client, machine.SystemID, d.Id())
	if err != nil {
		return diag.FromErr(err)
	}
	tfState := map[string]interface{}{
		"id":      fmt.Sprintf("%v", raid.ID),
		"machine": machine.SystemID,
		"name":    raid.Name,
		"uuid":    raid.UUID,
	}
	if err := setTerraformState(d, tfState); err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func resourceRaidUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*client.Client)

	machine, err := getMachine(client, d.Get("machine").(string))
	if err != nil {
		return diag.FromErr(err)
	}
	raid, err := getRaid(client, machine.SystemID, d.Id())
	if err != nil {
		return diag.FromErr(err)
	}
	if _, err := client.RAID.Update(machine.SystemID, raid.ID, getAddRaidParams(d)); err != nil {
		return diag.FromErr(err)
	}

	return resourceRaidRead(ctx, d, m)
}

func resourceRaidDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*client.Client)

	raid, err := getRaid(client, d.Get("machine").(string), d.Id())
	if err != nil {
		return diag.FromErr(err)
	}
	if err := client.RAID.Delete(d.Get("machine").(string), raid.ID); err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func getAddRaidParams(d *schema.ResourceData) *entity.RaidParams {
	return &entity.RaidParams{
		Name:               d.Get("name").(string),
		AddBlockDevices:    convertToStringSlice(d.Get("block_devices").([]interface{})),
		AddPartitions:      convertToStringSlice(d.Get("partitions").([]interface{})),
		AddSpareDevices:    convertToStringSlice(d.Get("spare_devices").([]interface{})),
		AddSparePartitions: convertToStringSlice(d.Get("spare_partitions").([]interface{})),
		UUID:               d.Get("uuid").(string),
	}
}

func getRemoveRaidParams(d *schema.ResourceData) *entity.RaidParams {
	return &entity.RaidParams{
		Name:                  d.Get("name").(string),
		RemoveBlockDevices:    convertToStringSlice(d.Get("block_devices").([]interface{})),
		RemovePartitions:      convertToStringSlice(d.Get("partitions").([]interface{})),
		RemoveSpareDevices:    convertToStringSlice(d.Get("spare_devices").([]interface{})),
		RemoveSparePartitions: convertToStringSlice(d.Get("spare_partitions").([]interface{})),
		UUID:                  d.Get("uuid").(string),
	}
}

func getRaidsParams(d *schema.ResourceData) *entity.RaidsParams {
	return &entity.RaidsParams{
		Name:            d.Get("name").(string),
		BlockDevices:    convertToStringSlice(d.Get("block_devices").([]interface{})),
		Level:           d.Get("level").(string),
		Partitions:      convertToStringSlice(d.Get("partitions").([]interface{})),
		SpareDevices:    convertToStringSlice(d.Get("spare_devices").([]interface{})),
		SparePartitions: convertToStringSlice(d.Get("spare_partitions").([]interface{})),
	}
}

func findRaid(client *client.Client, systemID string, identifier string) (*entity.Raid, error) {
	raids, err := client.RAIDs.Get(systemID)
	if err != nil {
		return nil, err
	}
	for _, v := range raids {
		if fmt.Sprintf("%v", v.ID) == identifier || v.Name == identifier {
			return &v, nil
		}
	}
	return nil, nil
}

func getRaid(client *client.Client, systemID string, identifier string) (*entity.Raid, error) {
	raid, err := findRaid(client, systemID, identifier)
	if err != nil {
		return nil, err
	}
	if raid == nil {
		return nil, fmt.Errorf("raid (%s) was not found", identifier)
	}
	return raid, nil
}
