import argparse
import os
import sys
import yaml
import pyarrow.orc as orc

# Mapping of PyArrow type strings to ClickHouse types
TYPE_MAPPING = {
    "string": "String",
    "int8": "Int8",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "uint8": "UInt8",
    "uint16": "UInt16",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "float": "Float32",
    "double": "Float64",
    "bool": "UInt8",
    "date32": "Date",
    "date64": "Date",
    "timestamp": "DateTime",
}

def map_arrow_type_to_clickhouse(arrow_type):
    type_str = str(arrow_type).lower()
    for prefix, ch_type in TYPE_MAPPING.items():
        if type_str.startswith(prefix):
            return ch_type
    return "String"

def inspect_orc_file(orc_path, table_name=None):
    if not os.path.exists(orc_path):
        raise FileNotFoundError(f"File not found: {orc_path}")
        
    orc_file = orc.ORCFile(orc_path)
    schema = orc_file.schema
    
    if not table_name:
        # Infer table name from path or filename
        table_name = os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(orc_path))))
        if not table_name or table_name in [".", "", "/"]:
            table_name = os.path.splitext(os.path.basename(orc_path))[0]

    columns = []
    for field in schema:
        ch_type = map_arrow_type_to_clickhouse(field.type)
        columns.append({
            "name": field.name,
            "raw_type": str(field.type),
            "clickhouse_type": ch_type
        })
        
    bronze_config = {
        "table_name": table_name,
        "layer": "bronze",
        "description": f"Auto-generated Bronze staging configuration for raw {table_name} dataset",
        "storage": {
            "format": "ORC",
            "file_pattern": f"{table_name}/organisation_id=*/processing_date=*/*.orc",
            "partitions": [
                {
                    "name": "organisation_id",
                    "type": "LowCardinality(String)",
                    "extract_regex": "organisation_id=([^/]+)"
                },
                {
                    "name": "processing_date",
                    "type": "Date",
                    "extract_regex": "processing_date=([^/]+)"
                }
            ]
        },
        "columns": columns,
        "engine": "MergeTree",
        "partition_by": "(processing_date, organisation_id)",
        "order_by": ["organisation_id", "processing_date"]
    }
    
    return bronze_config

def main():
    parser = argparse.ArgumentParser(
        description="Inspect raw ORC files and auto-generate draft Bronze YAML configurations from scratch."
    )
    parser.add_argument("file_path", help="Path to raw ORC file")
    parser.add_argument("--table-name", "-t", default=None, help="Explicit table name (optional)")
    parser.add_argument("--output", "-o", default=None, help="Output YAML file path (optional, prints to stdout if omitted)")
    
    args = parser.parse_args()
    
    try:
        config = inspect_orc_file(args.file_path, args.table_name)
        yaml_content = yaml.dump(config, sort_keys=False, default_flow_style=False)
        
        if args.output:
            os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
            with open(args.output, "w") as f:
                f.write(yaml_content)
            print(f"Successfully generated Bronze YAML configuration to {args.output}")
        else:
            print("--- Generated Bronze YAML Configuration ---")
            print(yaml_content)
    except Exception as e:
        print(f"Error inspecting file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
