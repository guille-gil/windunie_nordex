import os
import re
import pandas as pd
from datetime import datetime, timedelta

"""
Naming Conventions Enforced!
Folders must follow the format: <TurbineName>_<Component>_<SensorCode>

Example:
- DEN-TOL-N-89608_MainBearing_AI1
- DEN-TOL-N-89608_PlanetaryStage1_AI2

Breakdown:
- TurbineName: e.g., "DEN-TOL-N-89608"
- Component: MainBearing, PlanetaryStage1, PlanetaryStage2, HighSpeedShaft, 
  GeneratorDriveEnd, GeneratorNonDriveEnd
- SensorCode: AI# (e.g., AI1, AI2)
"""

# Define paths
raw_data_dir = "/Users/guillermogildeavallebellido/Desktop/Windunie/project_git/data/raw"
processed_data_dir = "/Users/guillermogildeavallebellido/Desktop/Windunie/project_git/data/processed"


def parse_vibration_file(file_path, turbine, component, sensor_code):
    """Parse a single vibration data file and return a DataFrame."""
    with open(file_path, 'r') as file:
        content = file.read()

    # Split on bracketed section headers of the form [something]
    sections = re.split(r'\[([^\]]+)\]', content)

    if len(sections) < 3:
        # Not enough sections to form metadata and data
        return pd.DataFrame()

    # Pair the sections: (section_name, section_content)
    paired_sections = list(zip(sections[1::2], sections[2::2]))

    all_data_frames = []
    metadata = {}

    for section_name, section_content in paired_sections:
        if 'aduchannel' in section_name.lower():
            # Parse metadata
            metadata = {}
            for line in section_content.strip().split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    metadata[key.strip()] = value.strip()

        elif 'adudata' in section_name.lower():
            # Parse data lines safely
            data_values = []
            for line in section_content.strip().split('\n'):
                line = line.strip()
                # Skip empty or comment lines
                if not line or line.startswith('#'):
                    continue
                try:
                    data_values.append(float(line))
                except ValueError:
                    continue

            if metadata.get('MeasUnit') == 'g':
                starttime = int(metadata.get('starttime', 0))
                sample_count = int(metadata.get('iSampleCnt', len(data_values)))
                frequency = float(metadata.get('iSampleRate', 0))

                # Generate timestamps based on starttime and frequency
                timestamps = [
                    datetime.fromtimestamp(starttime) + timedelta(seconds=i / frequency)
                    for i in range(sample_count)
                ]

                df = pd.DataFrame({
                    "timestamp": timestamps[:len(data_values)],
                    "vibration": data_values,
                    "component": metadata.get('szComponent', component),
                    "turbine": turbine,
                    "frequency": frequency,
                    "sensor_code": sensor_code,
                })
                all_data_frames.append(df)

    return pd.concat(all_data_frames, ignore_index=True) if all_data_frames else pd.DataFrame()


def process_folders():
    """Iterate through all folders and files to build the vibration dataset."""
    all_data = []

    for root, dirs, files in os.walk(raw_data_dir):
        folder_name = os.path.basename(root)

        # Skip the base 'raw' folder
        if folder_name == 'raw':
            continue

        # Ensure the folder conforms to the expected naming convention
        parts = folder_name.split('_')
        if len(parts) < 3:
            continue

        turbine = parts[0]
        component = parts[1]
        sensor_code = parts[2]

        for file in files:
            if file.endswith('.txt'):
                file_path = os.path.join(root, file)
                df = parse_vibration_file(file_path, turbine, component, sensor_code)
                if not df.empty:
                    all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # Sort by turbine, component, sensor_code, and timestamp
        final_df = final_df.sort_values(['turbine', 'component', 'sensor_code', 'timestamp'])

        # Group by turbine and save each turbine's data to a separate parquet file
        for turbine_name, group_df in final_df.groupby('turbine'):
            output_path = os.path.join(processed_data_dir, f"{turbine_name}_raw_vibration_data.parquet")
            group_df.to_parquet(output_path, index=False)
            print(f"Saved processed data for {turbine_name} to {output_path}")
    else:
        print("No vibration data found.")


if __name__ == "__main__":
    process_folders()