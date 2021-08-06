package aws

import (
	"fmt"
	// "log"
	// "reflect"
	// "time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lakeformation"
	// "github.com/hashicorp/aws-sdk-go-base/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/hashcode"
	iamwaiter "github.com/terraform-providers/terraform-provider-aws/aws/internal/service/iam/waiter"
	// tflakeformation "github.com/terraform-providers/terraform-provider-aws/aws/internal/service/lakeformation"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/service/lakeformation/waiter"
)

func resourceAwsLakeFormationLFTagAssociation() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsLakeFormationLFTagAssociationCreate,
		Read:   resourceAwsLakeFormationLFTagAssociationRead,
		Update: resourceAwsLakeFormationLFTagAssociationCreate,
		Delete: resourceAwsLakeFormationLFTagAssociationDelete,

		Schema: map[string]*schema.Schema{
			"catalog_id": {
				Type:         schema.TypeString,
				ForceNew:     true,
				Optional:     true,
				ValidateFunc: validateAwsAccountId,
			},
			"database": {
				Type:     schema.TypeList,
				Optional: true,
				Computed: true,
				MaxItems: 1,
				ExactlyOneOf: []string{
					"database",
					"table",
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"catalog_id": {
							Type:         schema.TypeString,
							Optional:     true,
							Computed:     true,
							ValidateFunc: validateAwsAccountId,
						},
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
			"table": {
				Type:     schema.TypeList,
				Computed: true,
				ForceNew: true,
				MaxItems: 1,
				Optional: true,
				ExactlyOneOf: []string{
					"database",
					"table",
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"catalog_id": {
							Type:         schema.TypeString,
							Computed:     true,
							ForceNew:     true,
							Optional:     true,
							ValidateFunc: validateAwsAccountId,
						},
						"database_name": {
							Type:     schema.TypeString,
							ForceNew: true,
							Required: true,
						},
						"name": {
							Type:     schema.TypeString,
							Computed: true,
							ForceNew: true,
							Optional: true,
							AtLeastOneOf: []string{
								"table.0.name",
								"table.0.wildcard",
							},
						},
						"wildcard": {
							Type:     schema.TypeBool,
							Default:  false,
							ForceNew: true,
							Optional: true,
							AtLeastOneOf: []string{
								"table.0.name",
								"table.0.wildcard",
							},
						},
					},
				},
			},
			"lf_tag": {
				Type:     schema.TypeList,
				Optional: true,
				Computed: true,
				MinItems: 1,
				MaxItems: 50,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"catalog_id": {
							Type:     schema.TypeString,
							ForceNew: true,
							Optional: true,
							Computed: true,
						},
						"key": {
							Type:         schema.TypeString,
							Required:     true,
							ForceNew:     true,
							ValidateFunc: validation.StringLenBetween(1, 128),
						},
						"values": {
							Type:     schema.TypeSet,
							Required: true,
							MinItems: 1,
							MaxItems: 15,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: validateLFTagValues(),
							},
							Set: schema.HashString,
						},
					},
				},
			},
		},
	}
}

func resourceAwsLakeFormationLFTagAssociationCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).lakeformationconn

	input := &lakeformation.AddLFTagsToResourceInput{
		Resource: &lakeformation.Resource{},
	}

	if v, ok := d.GetOk("catalog_id"); ok {
		input.CatalogId = aws.String(v.(string))
	}

	if v, ok := d.GetOk("lf_tag"); ok && len(v.([]interface{})) > 0 {
		input.LFTags = expandLakeFormationLFTagPairs(v.([]interface{}))
	}

	if v, ok := d.GetOk("database"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Database = expandLakeFormationDatabaseResource(v.([]interface{})[0].(map[string]interface{}))
	}

	if v, ok := d.GetOk("table"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Table = expandLakeFormationTableResource(v.([]interface{})[0].(map[string]interface{}))
	}

	var output *lakeformation.AddLFTagsToResourceOutput
	err := resource.Retry(iamwaiter.PropagationTimeout, func() *resource.RetryError {
		var err error
		output, err = conn.AddLFTagsToResource(input)
		if err != nil {
			// if tfawserr.ErrMessageContains(err, lakeformation.ErrCodeInvalidInputException, "Invalid principal") {
			// 	return resource.RetryableError(err)
			// }
			// if tfawserr.ErrMessageContains(err, lakeformation.ErrCodeInvalidInputException, "Grantee has no permissions") {
			// 	return resource.RetryableError(err)
			// }
			// if tfawserr.ErrMessageContains(err, lakeformation.ErrCodeInvalidInputException, "register the S3 path") {
			// 	return resource.RetryableError(err)
			// }
			// if tfawserr.ErrCodeEquals(err, lakeformation.ErrCodeConcurrentModificationException) {
			// 	return resource.RetryableError(err)
			// }
			// if tfawserr.ErrMessageContains(err, "AccessDeniedException", "is not authorized to access requested permissions") {
			// 	return resource.RetryableError(err)
			// }

			return resource.NonRetryableError(fmt.Errorf("error adding LF-Tags to resource: %w", err))
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error adding LF-Tags to resource (input: %v): %w", input, err)
	}

	d.SetId(fmt.Sprintf("%d", hashcode.String(input.String())))

	return resourceAwsLakeFormationLFTagAssociationRead(d, meta)
}

func resourceAwsLakeFormationLFTagAssociationRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).lakeformationconn

	showAssignedLFTags := true

	input := &lakeformation.GetResourceLFTagsInput{
		Resource:           &lakeformation.Resource{},
		ShowAssignedLFTags: &showAssignedLFTags, // I have no idea what this does
	}

	if v, ok := d.GetOk("catalog_id"); ok {
		input.CatalogId = aws.String(v.(string))
	}

	if v, ok := d.GetOk("database"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Database = expandLakeFormationDatabaseResource(v.([]interface{})[0].(map[string]interface{}))
	}

	// tableType may be unnecessary here?
	// tableType := ""

	if v, ok := d.GetOk("table"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Table = expandLakeFormationTableResource(v.([]interface{})[0].(map[string]interface{}))
		// tableType = tflakeformation.TableTypeTable
	}

	// TODO: retries?
	output, err := conn.GetResourceLFTags(input)

	if err != nil {
		return fmt.Errorf("can't get resource LF-Tags (input: %v): %w", input, err)
	}

	if len(output.LFTagOnDatabase) > 0 {
		fmt.Printf("Found %d tags on Database resource", len(output.LFTagOnDatabase))
		if err := d.Set("lf_tag", flattenLakeFormationLFTagPairs(output.LFTagOnDatabase)); err != nil {
			return fmt.Errorf("error on database thing ???: %w", err)
		}
	}

	if len(output.LFTagsOnTable) > 0 {
		fmt.Printf("Found %d tags on Table resource", len(output.LFTagsOnTable))
		if err := d.Set("lf_tag", flattenLakeFormationLFTagPairs(output.LFTagsOnTable)); err != nil {
			return fmt.Errorf("error on table thing ???: %w", err)
		}
	}

	return nil
}

func resourceAwsLakeFormationLFTagAssociationDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).lakeformationconn

	input := &lakeformation.RemoveLFTagsFromResourceInput{
		Resource: &lakeformation.Resource{},
	}

	if v, ok := d.GetOk("catalog_id"); ok {
		input.CatalogId = aws.String(v.(string))
	}

	if v, ok := d.GetOk("lf_tag"); ok && len(v.([]interface{})) > 0 {
		input.LFTags = expandLakeFormationLFTagPairs(v.([]interface{}))
	}

	if v, ok := d.GetOk("database"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Database = expandLakeFormationDatabaseResource(v.([]interface{})[0].(map[string]interface{}))
	}

	if v, ok := d.GetOk("table"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Table = expandLakeFormationTableResource(v.([]interface{})[0].(map[string]interface{}))
	}

	err := resource.Retry(waiter.PermissionsDeleteRetryTimeout, func() *resource.RetryError {
		var err error
		_, err = conn.RemoveLFTagsFromResource(input)
		if err != nil {
			// if tfawserr.ErrMessageContains(err, lakeformation.ErrCodeInvalidInputException, "register the S3 path") {
			// 	return resource.RetryableError(err)
			// }
			// if tfawserr.ErrCodeEquals(err, lakeformation.ErrCodeConcurrentModificationException) {
			// 	return resource.RetryableError(err)
			// }
			// if tfawserr.ErrMessageContains(err, "AccessDeniedException", "is not authorized to access requested permissions") {
			// 	return resource.RetryableError(err)
			// }
			return resource.NonRetryableError(fmt.Errorf("unable to remove LF-Tags from resource: %w", err))
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("unable to remove LF-Tags from resource (input: %v): %w", input, err)
	}

	return nil
}

func expandLakeFormationLFTagPairs(tags []interface{}) []*lakeformation.LFTagPair {
	tagSlice := []*lakeformation.LFTagPair{}
	for _, element := range tags {
		elementMap := element.(map[string]interface{})

		tag := &lakeformation.LFTagPair{
			TagKey:    aws.String(elementMap["key"].(string)),
			TagValues: expandStringSet(elementMap["values"].(*schema.Set)),
		}

		tagSlice = append(tagSlice, tag)
	}
	return tagSlice
}

func flattenLakeFormationLFTagPairs(tags []*lakeformation.LFTagPair) []map[string]interface{} {
	tagSlice := make([]map[string]interface{}, len(tags))
	if len(tags) > 0 {
		for i, t := range tags {
			tag := make(map[string]interface{})

			if v := aws.StringValue(t.TagKey); v != "" {
				tag["key"] = v
			}

			if v := t.TagValues; v != nil {
				tag["values"] = flattenStringSet(v)
			}

			tagSlice[i] = tag
		}
	}
	return tagSlice
}
