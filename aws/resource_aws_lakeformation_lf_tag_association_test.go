package aws

import (
	"fmt"
	"reflect"
	"strconv"
	// "strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lakeformation"
	"github.com/hashicorp/aws-sdk-go-base/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	iamwaiter "github.com/terraform-providers/terraform-provider-aws/aws/internal/service/iam/waiter"
	tflakeformation "github.com/terraform-providers/terraform-provider-aws/aws/internal/service/lakeformation"
)

func TestAccAWSLakeFormationLFTagAssociation_basic(t *testing.T) {
	resourceName := "aws_lakeformation_lf_tag_association.test"
	rKey := acctest.RandomWithPrefix("tf-acc-test")
	dbName := "aws_glue_catalog_database.test"
	tagName := "aws_lakeformation_lf_tag.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t); testAccPartitionHasServicePreCheck(lakeformation.EndpointsID, t) },
		ErrorCheck:   testAccErrorCheck(t, lakeformation.EndpointsID),
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSLakeFormationLFTagAssociationsDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSLakeFormationLFTagAssociationConfig_basic(rKey),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSLakeFormationLFTagAssociationsExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "database.#", "1"),
					resource.TestCheckResourceAttrPair(resourceName, "database.0.name", dbName, "name"),
					resource.TestCheckResourceAttr(resourceName, "lf_tag.#", "1"),
					resource.TestCheckResourceAttrPair(resourceName, "lf_tag.0.key", tagName, "key"),
					resource.TestCheckResourceAttrPair(resourceName, "lf_tag.0.values", tagName, "values"),
				),
			},
		},
	})
}

func TestAccAWSLakeFormationLFTagAssociation_disappears(t *testing.T) {
	resourceName := "aws_lakeformation_lf_tag_association.test"
	rKey := acctest.RandomWithPrefix("tf-acc-test")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t); testAccPartitionHasServicePreCheck(lakeformation.EndpointsID, t) },
		ErrorCheck:   testAccErrorCheck(t, lakeformation.EndpointsID),
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSLakeFormationLFTagAssociationsDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSLakeFormationLFTagAssociationConfig_basic(rKey),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSLakeFormationLFTagAssociationsExists(resourceName),
					testAccCheckResourceDisappears(testAccProvider, resourceAwsLakeFormationLFTagAssociation(), resourceName),
				),
				ExpectNonEmptyPlan: true,
			},
		},
	})
}

func TestAccAWSLakeFormationLFTagAssociation_table(t *testing.T) {
	resourceName := "aws_lakeformation_lf_tag_association.test"
	rKey := acctest.RandomWithPrefix("tf-acc-test")
	tableName := "aws_glue_catalog_table.test"
	tagName := "aws_lakeformation_lf_tag.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t); testAccPartitionHasServicePreCheck(lakeformation.EndpointsID, t) },
		ErrorCheck:   testAccErrorCheck(t, lakeformation.EndpointsID),
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSLakeFormationLFTagAssociationsDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSLakeFormationLFTagAssociationConfig_table(rKey),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSLakeFormationLFTagAssociationsExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "table.#", "1"),
					resource.TestCheckResourceAttrPair(resourceName, "table.0.database_name", tableName, "database_name"),
					resource.TestCheckResourceAttrPair(resourceName, "table.0.name", tableName, "name"),
					resource.TestCheckResourceAttr(resourceName, "lf_tag.#", "1"),
					resource.TestCheckResourceAttrPair(resourceName, "lf_tag.0.key", tagName, "key"),
					resource.TestCheckResourceAttrPair(resourceName, "lf_tag.0.values", tagName, "values"),
				),
			},
		},
	})
}

func TestAccAWSLakeFormationLFTagAssociation_table_with_columns(t *testing.T) {
	resourceName := "aws_lakeformation_lf_tag_association.test"
	rKey := acctest.RandomWithPrefix("tf-acc-test")
	tableName := "aws_glue_catalog_table.test"
	tagName := "aws_lakeformation_lf_tag.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t); testAccPartitionHasServicePreCheck(lakeformation.EndpointsID, t) },
		ErrorCheck:   testAccErrorCheck(t, lakeformation.EndpointsID),
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSLakeFormationLFTagAssociationsDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSLakeFormationLFTagAssociationConfig_table_with_columns(rKey),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSLakeFormationLFTagAssociationsExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "table_with_columns.#", "1"),
					resource.TestCheckResourceAttrPair(resourceName, "table_with_columns.0.database_name", tableName, "database_name"),
					resource.TestCheckResourceAttrPair(resourceName, "table_with_columns.0.name", tableName, "name"),
					resource.TestCheckResourceAttr(resourceName, "table_with_columns.0.column_names.#", "2"),
					resource.TestCheckResourceAttr(resourceName, "table_with_columns.0.column_names.0", "first"),
					resource.TestCheckResourceAttr(resourceName, "table_with_columns.0.column_names.0", "second"),
					resource.TestCheckResourceAttr(resourceName, "lf_tag.#", "1"),
					resource.TestCheckResourceAttrPair(resourceName, "lf_tag.0.key", tagName, "key"),
					resource.TestCheckResourceAttrPair(resourceName, "lf_tag.0.values", tagName, "values"),
				),
			},
		},
	})
}

func testAccCheckAWSLakeFormationLFTagAssociationsDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*AWSClient).lakeformationconn

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aws_lakeformation_lf_tag_association" {
			continue
		}

		tagCount, err := LFTagsCountForLakeFormationResource(conn, rs)

		if err != nil {
			return fmt.Errorf("error getting LF-Tags on resource (%s): %w", rs.Primary.ID, err)
		}

		if tagCount != 0 {
			return fmt.Errorf("LF-Tag associations (%s) still exist: %d", rs.Primary.ID, tagCount)
		}

		return nil
	}

	return nil
}

func testAccCheckAWSLakeFormationLFTagAssociationsExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}

		conn := testAccProvider.Meta().(*AWSClient).lakeformationconn

		tagCount, err := LFTagsCountForLakeFormationResource(conn, rs)

		if err != nil {
			return fmt.Errorf("error getting LF-Tags on resource (%s): %w", rs.Primary.ID, err)
		}

		if tagCount == 0 {
			return fmt.Errorf("LF-Tag associations (%s) do not exist or could not be found", rs.Primary.ID)
		}

		return nil
	}
}

func LFTagsCountForLakeFormationResource(conn *lakeformation.LakeFormation, rs *terraform.ResourceState) (int, error) {
	input := &lakeformation.GetResourceLFTagsInput{
		Resource: &lakeformation.Resource{},
	}

	if v, ok := rs.Primary.Attributes["catalog_id"]; ok && v != "" {
		input.CatalogId = aws.String(v)
	}

	resourceType := ""

	if v, ok := rs.Primary.Attributes["database.#"]; ok && v != "" && v != "0" {
		tfMap := map[string]interface{}{}

		if v := rs.Primary.Attributes["database.0.catalog_id"]; v != "" {
			tfMap["catalog_id"] = v
		}

		if v := rs.Primary.Attributes["database.0.name"]; v != "" {
			tfMap["name"] = v
		}

		input.Resource.Database = expandLakeFormationDatabaseResource(tfMap)
		resourceType = tflakeformation.DatabaseResourceType
	}

	if v, ok := rs.Primary.Attributes["table.#"]; ok && v != "" && v != "0" {
		tfMap := map[string]interface{}{}

		if v := rs.Primary.Attributes["table.0.catalog_id"]; v != "" {
			tfMap["catalog_id"] = v
		}

		if v := rs.Primary.Attributes["table.0.database_name"]; v != "" {
			tfMap["database_name"] = v
		}

		if v := rs.Primary.Attributes["table.0.name"]; v != "" {
			tfMap["name"] = v
		}

		if v := rs.Primary.Attributes["table.0.wildcard"]; v != "" && v == "true" {
			tfMap["wildcard"] = true
		}

		input.Resource.Table = expandLakeFormationTableResource(tfMap)
		resourceType = tflakeformation.TableResourceType
	}

	if v, ok := rs.Primary.Attributes["table_with_columns.#"]; ok && v != "" && v != "0" {
		tfMap := map[string]interface{}{}

		if v := rs.Primary.Attributes["table_with_columns.0.catalog_id"]; v != "" {
			tfMap["catalog_id"] = v
		}

		if v := rs.Primary.Attributes["table_with_columns.0.database_name"]; v != "" {
			tfMap["database_name"] = v
		}

		if v := rs.Primary.Attributes["table_with_columns.0.name"]; v != "" {
			tfMap["name"] = v
		}

		var columnNames []string
		if cols, err := strconv.Atoi(rs.Primary.Attributes["table_with_columns.0.column_names.#"]); err == nil {
			for i := 0; i < cols; i++ {
				columnNames = append(columnNames, rs.Primary.Attributes[fmt.Sprintf("table_with_columns.0.column_names.%d", i)])
			}
		}
		tfMap["column_names"] = aws.StringSlice(columnNames)

		// input.Resource.TableWithColumns = expandLakeFormationTableWithColumnsResource(v.([]interface{})[0].(map[string]interface{}))
		input.Resource.TableWithColumns = expandLakeFormationTableWithColumnsResource(tfMap)
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

	if tfawserr.ErrCodeEquals(err, lakeformation.ErrCodeEntityNotFoundException) {
		return 0, nil
	}

	if tfawserr.ErrMessageContains(err, "InvalidInputException", "not found") {
		return 0, nil
	}

	if tfawserr.ErrMessageContains(err, "AccessDeniedException", "Resource does not exist") {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("can't get resource LF-Tags (input: %v): %w", input, err)
	}

	if resourceType == tflakeformation.DatabaseResourceType {
		return len(output.LFTagOnDatabase), nil
	}

	if resourceType == tflakeformation.TableResourceType {
		return len(output.LFTagsOnTable), nil
	}

	if resourceType == tflakeformation.TableWithColumnsResourceType {
		// Since a common set of LF-Tags is applied to list of columns, we should expect each element in output.LFTagsOnColumns to be equal
		for i := 1; i < len(output.LFTagsOnColumns); i++ {
			if !reflect.DeepEqual(output.LFTagsOnColumns[0].LFTags, output.LFTagsOnColumns[i].LFTags) {
				return 0, fmt.Errorf("Expected common LF-Tags for all columns, instead %v is different to %v", output.LFTagsOnColumns[0].LFTags, output.LFTagsOnColumns[i].LFTags)
			}
		}
		return len(output.LFTagsOnColumns), nil
	}

	return 0, fmt.Errorf("No valid resource type was specified")
}

func testAccAWSLakeFormationLFTagAssociationConfig_basic(rKey string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

resource "aws_lakeformation_data_lake_settings" "test" {
  admins = [data.aws_caller_identity.current.arn]
}

resource "aws_glue_catalog_database" "test" {
  name = %[1]q
}

resource "aws_lakeformation_lf_tag" "test" {
  key    = %[1]q
  values = ["value"]

  # for consistency, ensure that admins are setup before testing
  depends_on = [aws_lakeformation_data_lake_settings.test]
}

resource "aws_lakeformation_lf_tag_association" "test" {
  database {
	name = aws_glue_catalog_database.test.name
  }
  
  lf_tag {
	key    = aws_lakeformation_lf_tag.test.key
	values = aws_lakeformation_lf_tag.test.values
  }
  
  depends_on = [aws_lakeformation_data_lake_settings.test]
}
`, rKey)
}

func testAccAWSLakeFormationLFTagAssociationConfig_table(rKey string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

resource "aws_lakeformation_data_lake_settings" "test" {
  admins = [data.aws_caller_identity.current.arn]
}

resource "aws_glue_catalog_database" "test" {
  name = %[1]q
}

resource "aws_glue_catalog_table" "test" {
  name          = %[1]q
  database_name = aws_glue_catalog_database.test.name
}

resource "aws_lakeformation_lf_tag" "test" {
  key    = %[1]q
  values = ["value"]

  # for consistency, ensure that admins are setup before testing
  depends_on = [aws_lakeformation_data_lake_settings.test]
}

resource "aws_lakeformation_lf_tag_association" "test" {
  table {
	database_name = aws_glue_catalog_database.test.name
	name          = aws_glue_catalog_table.test.name
  }
  
  lf_tag {
	key    = aws_lakeformation_lf_tag.test.key
	values = aws_lakeformation_lf_tag.test.values
  }
  
  depends_on = [aws_lakeformation_data_lake_settings.test]
}
`, rKey)
}

func testAccAWSLakeFormationLFTagAssociationConfig_table_with_columns(rKey string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

resource "aws_lakeformation_data_lake_settings" "test" {
  admins = [data.aws_caller_identity.current.arn]
}

resource "aws_glue_catalog_database" "test" {
  name = %[1]q
}

resource "aws_glue_catalog_table" "test" {
  name          = %[1]q
  database_name = aws_glue_catalog_database.test.name

  storage_descriptor {
	columns {
		name = "first"
		type = "string"
	}

	columns {
		name = "second"
		type = "string"
	}
  }
}

resource "aws_lakeformation_lf_tag" "test" {
  key    = %[1]q
  values = ["value"]

  # for consistency, ensure that admins are setup before testing
  depends_on = [aws_lakeformation_data_lake_settings.test]
}

resource "aws_lakeformation_lf_tag_association" "test" {
  table_with_columns {
	database_name = aws_glue_catalog_database.test.name
	name          = aws_glue_catalog_table.test.name
	column_names  = ["first", "second"]
  }
  
  lf_tag {
	key    = aws_lakeformation_lf_tag.test.key
	values = aws_lakeformation_lf_tag.test.values
  }
  
  depends_on = [aws_lakeformation_data_lake_settings.test]
}
`, rKey)
}
