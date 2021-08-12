---
subcategory: "Lake Formation"
layout: "aws"
page_title: "AWS: aws_lakeformation_lf_tag_association"
description: |-
    Attaches LF-Tags to Glue resources
---

# Resource: aws_lakeformation_lf_tag_association

Attaches one or more LF-Tags to an existing Glue resource. Only one value can be attached per LF-Tag. The maximum number of LF-tags that can be attached to a Glue resource is 50.

## Example Usage

```terraform
resource "aws_lf_tag" "department" {
  key = "Department"
  values = ["Sales", "Marketing", "Engineering"]
}

resource "aws_lf_tag" "environment" {
  key = "Environment"
  values = ["Development", "Production"]
}

resource "aws_lakeformation_lf_tag_association" "example" {
  table {
    database_name = aws_glue_catalog_table.example.database_name
    name          = aws_glue_catalog_table.example.name
  }

  lf_tag {
      key    = aws_lf_tag.department.key
      values = ["Sales"]
  }

  lf_tag {
      key    = aws_lf_tag.department.environment
      values = ["Production"]
  }
}
```

## Argument Reference

The following arguments are supported:

* `catalog_id` - (Optional) ID of the Data Catalog to create the tag in. If omitted, this defaults to the AWS Account ID.
* `lf_tag` - (Required) Configuration block for LF-Tags to associate. Can have up to 50 blocks. Detailed below.

One of the following is required:

* `database` - (Optional) Configuration block for a database resource. Detailed below.
* `table` - (Optional) Configuration block for a table resource. Detailed below.
* `table_with_columns` - (Optional) Configuration block for a table with columns resource. Detailed below.

### database

The following argument is required:

* `name` – (Required) Name of the database resource. Unique to the Data Catalog.

The following argument is optional:

* `catalog_id` - (Optional) Identifier for the Data Catalog. By default, it is the account ID of the caller.

### lf_tag

The following arguments are required:

* `key` - (Required) The key-name for the LF-Tag.
* `values` - (Required) A list of possible values an attribute can take.

### table

The following arguments are required:

* `database_name` – (Required) Name of the database for the table. Unique to a Data Catalog.
* `name` - (Required) Name of the table.

The following arguments are optional:

* `catalog_id` - (Optional) Identifier for the Data Catalog. By default, it is the account ID of the caller.

### table_with_columns

The following arguments are required:

* `column_names` - (Required) Set of column names for the table.
* `database_name` – (Required) Name of the database for the table with columns resource. Unique to the Data Catalog.
* `name` – (Required) Name of the table resource.

## Attributes Reference

In addition to all arguments above, the following attributes are exported:

* `id` - Catalog ID and key-name of the tag

## Import

Lake Formation LF-Tags can be imported using the `catalog_id:key`. If you have not set a Catalog ID specify the AWS Account ID that the database is in, e.g.

```
$ terraform import aws_lakeformation_lf_tag.example 123456789012:some_key
```

