package aws

import (
	"fmt"
	"log"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lakeformation"
	"github.com/hashicorp/aws-sdk-go-base/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/hashcode"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/service/lakeformation/waiter"
	iamwaiter "github.com/terraform-providers/terraform-provider-aws/aws/internal/service/iam/waiter"
	tflakeformation "github.com/terraform-providers/terraform-provider-aws/aws/internal/service/lakeformation"
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
					"table_with_columns",
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
					"table_with_columns",
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
							ForceNew: true,
							Required: true,
						},
					},
				},
			},
			"table_with_columns": {
				Type:     schema.TypeList,
				Computed: true,
				ForceNew: true,
				MaxItems: 1,
				Optional: true,
				ExactlyOneOf: []string{
					"database",
					"table",
					"table_with_columns",
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
						// TODO: Add to docs that excluded_column_names is not permitted when assigning tags
						"column_names": {
							Type:     schema.TypeSet,
							ForceNew: true,
							Required: true,
							Set:      schema.HashString,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: validation.NoZeroValues,
							},
						},
						"database_name": {
							Type:     schema.TypeString,
							ForceNew: true,
							Required: true,
						},
						"name": {
							Type:     schema.TypeString,
							ForceNew: true,
							Required: true,
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
							// Can only assign a single tag value to resources - this could be a TypeString but kept as a list for consistency with API
							MaxItems: 1,
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

	if v, ok := d.GetOk("table_with_columns"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.TableWithColumns = expandLakeFormationTableWithColumnsResource(v.([]interface{})[0].(map[string]interface{}))
	}

	var output *lakeformation.AddLFTagsToResourceOutput
	err := resource.Retry(iamwaiter.PropagationTimeout, func() *resource.RetryError {
		var err error
		output, err = conn.AddLFTagsToResource(input)
		if err != nil {
			if tfawserr.ErrCodeEquals(err, lakeformation.ErrCodeConcurrentModificationException) {
				return resource.RetryableError(err)
			}
			if tfawserr.ErrMessageContains(err, "AccessDeniedException", "is not authorized to access requested permissions") {
				return resource.RetryableError(err)
			}

			return resource.NonRetryableError(fmt.Errorf("error adding LF-Tags to resource: %w", err))
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error adding LF-Tags to resource (input: %v): %w", input, err)
	}

	if output == nil {
		return fmt.Errorf("error adding LF-Tags to resource: empty response")
	}

	if len(output.Failures) > 0 {
		return fmt.Errorf("%d failure(s) when adding LF-Tags to resource: %v", len(output.Failures), output.Failures)
	}

	d.SetId(fmt.Sprintf("%d", hashcode.String(input.String())))

	return resourceAwsLakeFormationLFTagAssociationRead(d, meta)
}

func resourceAwsLakeFormationLFTagAssociationRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).lakeformationconn

	// Ensures we only see assigned tags and not inherited ones
	showAssignedLFTags := true

	input := &lakeformation.GetResourceLFTagsInput{
		Resource:           &lakeformation.Resource{},
		ShowAssignedLFTags: &showAssignedLFTags,
	}

	if v, ok := d.GetOk("catalog_id"); ok {
		input.CatalogId = aws.String(v.(string))
	}

	resourceType := ""

	if v, ok := d.GetOk("database"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Database = expandLakeFormationDatabaseResource(v.([]interface{})[0].(map[string]interface{}))
		resourceType = tflakeformation.DatabaseResourceType
	}

	if v, ok := d.GetOk("table"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.Table = expandLakeFormationTableResource(v.([]interface{})[0].(map[string]interface{}))
		resourceType = tflakeformation.TableResourceType
	}

	if v, ok := d.GetOk("table_with_columns"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.TableWithColumns = expandLakeFormationTableWithColumnsResource(v.([]interface{})[0].(map[string]interface{}))
		resourceType = tflakeformation.TableWithColumnsResourceType
	}

	var output *lakeformation.GetResourceLFTagsOutput
	err := resource.Retry(iamwaiter.PropagationTimeout, func() *resource.RetryError {
		var err error
		output, err = conn.GetResourceLFTags(input)
		if err != nil {
			if tfawserr.ErrMessageContains(err, "AccessDeniedException", "is not authorized to access requested permissions") {
				return resource.RetryableError(err)
			}

			return resource.NonRetryableError(fmt.Errorf("error getting LF-Tags on resource: %w", err))
		}
		return nil
	})

	if !d.IsNewResource() {
		if tfawserr.ErrCodeEquals(err, lakeformation.ErrCodeEntityNotFoundException) {
			log.Printf("[WARN] Resource LF-Tag association (%s) not found, removing from state", d.Id())
			d.SetId("")
			return nil
		}

		if tfawserr.ErrMessageContains(err, "AccessDeniedException", "Resource does not exist") {
			log.Printf("[WARN] Resource LF-Tag association (%s) not found, removing from state: %s", d.Id(), err)
			d.SetId("")
			return nil
		}

		if len(output.LFTagOnDatabase) == 0 && len(output.LFTagsOnTable) == 0 && len(output.LFTagsOnColumns) == 0 {
			log.Printf("[WARN] Resource LF-Tag association (%s) not found, removing from state (0 LF-Tags)", d.Id())
			d.SetId("")
			return nil
		}
	}

	if err != nil {
		return fmt.Errorf("can't get resource LF-Tags (input: %v): %w", input, err)
	}

	if len(output.LFTagOnDatabase) == 0 && len(output.LFTagsOnTable) == 0 && len(output.LFTagsOnColumns) == 0 {
		log.Printf("[WARN] No LF-Tag associations (%s) found", d.Id())
		d.Set("database", nil)
		d.Set("table", nil)
		d.Set("table_with_columns", nil)
		return nil
	}

	if resourceType == tflakeformation.DatabaseResourceType && len(output.LFTagOnDatabase) > 0 {
		if err := d.Set("lf_tag", flattenLakeFormationLFTagPairs(output.LFTagOnDatabase)); err != nil {
			return fmt.Errorf("error setting LF-tags on database resource: %w", err)
		}
	}

	if resourceType == tflakeformation.TableResourceType && len(output.LFTagsOnTable) > 0 {
		if err := d.Set("lf_tag", flattenLakeFormationLFTagPairs(output.LFTagsOnTable)); err != nil {
			return fmt.Errorf("error setting LF-tags on table resource: %w", err)
		}
	}

	if resourceType == tflakeformation.TableWithColumnsResourceType && len(output.LFTagsOnColumns) > 0 {
		// Since a common set of LF-Tags is applied to list of columns, we should expect each output.LFTagsOnColumns[?].LFTags to be equal
		left := output.LFTagsOnColumns[0].LFTags
		for i := 1; i < len(output.LFTagsOnColumns); i++ {
			right := output.LFTagsOnColumns[i].LFTags
			if !reflect.DeepEqual(left, right) {
				return fmt.Errorf("Expected common LF-Tags for all columns, instead %v is different to %v", left, right)
			}
		}

		if err := d.Set("lf_tag", flattenLakeFormationLFTagPairs(output.LFTagsOnColumns[0].LFTags)); err != nil {
			return fmt.Errorf("error setting LF-tags on table_with_columns resource: %w", err)
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

	if v, ok := d.GetOk("table_with_columns"); ok && len(v.([]interface{})) > 0 && v.([]interface{})[0] != nil {
		input.Resource.TableWithColumns = expandLakeFormationTableWithColumnsResource(v.([]interface{})[0].(map[string]interface{}))
	}

	var output *lakeformation.RemoveLFTagsFromResourceOutput
	err := resource.Retry(waiter.PermissionsDeleteRetryTimeout, func() *resource.RetryError {
		var err error
		output, err = conn.RemoveLFTagsFromResource(input)
		if err != nil {
			if tfawserr.ErrCodeEquals(err, lakeformation.ErrCodeConcurrentModificationException) {
				return resource.RetryableError(err)
			}
			if tfawserr.ErrMessageContains(err, "AccessDeniedException", "is not authorized to access requested permissions") {
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(fmt.Errorf("error removing LF-Tags from resource: %w", err))
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("unable to remove LF-Tags from resource (input: %v): %w", input, err)
	}

	if output == nil {
		return fmt.Errorf("error removing LF-Tags to resource: empty response")
	}

	if len(output.Failures) > 0 {
		return fmt.Errorf("%d failure(s) when removing LF-Tags from resource: %v", len(output.Failures), output.Failures)
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

func flattenLakeFormationColumnLFTags(tags []*lakeformation.ColumnLFTag) []map[string]interface{} {
	tagSlice := make([]map[string]interface{}, len(tags))
	if len(tags) > 0 {
		for i, v := range tags {
			tagSlice[i] = flattenLakeFormationColumnLFTag(v)
		}
	}

	return tagSlice
}

func flattenLakeFormationColumnLFTag(ct *lakeformation.ColumnLFTag) map[string]interface{} {
	columnTag := make(map[string]interface{})

	if ct == nil {
		return columnTag
	}

	if v := aws.StringValue(ct.Name); v != "" {
		columnTag["name"] = v
	}

	if v := ct.LFTags; v != nil {
		columnTag["lf_tag"] = flattenLakeFormationLFTagPairs(v)
	}

	return columnTag
}
