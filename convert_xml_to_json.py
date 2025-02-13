import xmltodict
import json
import argparse
from pathlib import Path

def convert_xml_to_json(input_xml_path: str) -> None:
    """
    Convert XML file to JSON format
    
    Args:
        input_xml_path: Path to input XML file
    """
    input_path = Path(input_xml_path)
    output_path = input_path.with_suffix('.json')
    
    with open(input_path, 'r', encoding='utf-8') as xml_file:
        xml_content = xml_file.read()
    
    xml_dict = xmltodict.parse(xml_content)
    json_content = json.dumps(xml_dict, indent=2)
    
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json_file.write(json_content)
        
    print(f"Converted: {input_path} -> {output_path}")

def process_directory(directory: str) -> None:
    """
    Process all XML files in directory and subdirectories
    
    Args:
        directory: Root directory to start search
    """
    root_path = Path(directory)
    xml_files = [f for f in root_path.rglob("*.xml") if not f.name.startswith("~$")]
    
    if not xml_files:
        print(f"No XML files found in {directory}")
        return
        
    print(f"Found {len(xml_files)} XML files")
    for xml_file in xml_files:
        try:
            convert_xml_to_json(xml_file)
        except Exception as e:
            print(f"Error processing {xml_file}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Convert XML files to JSON')
    parser.add_argument('directory', help='Directory containing XML files')
    
    args = parser.parse_args()
    process_directory(args.directory)

if __name__ == "__main__":
    main()