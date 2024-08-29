import os
import zipfile
import time

def create_zips(input_folder, veh_guid, imei):
    # Get all image files in the input folder
    image_files = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

    # Create zip files in batches of 10
    for i in range(0, len(image_files), 10):
        # Generate a timestamp
        timestamp = str(int(time.time()))
        
        # Create the zip filename
        zip_filename = f"{veh_guid}_{imei}_{timestamp}_{i//10 + 1}.zip"
        zip_filepath = os.path.join(input_folder, zip_filename)
        
        # Create a new zip file
        with zipfile.ZipFile(zip_filepath, 'w') as zipf:
            for image_file in image_files[i:i + 10]:
                zipf.write(os.path.join(input_folder, image_file), image_file)
        
        print(f"Created {zip_filename}")

# Example usage
input_folder = "/home/narravenkataraghucharan/Downloads/Spy-2/Spy"  # Replace with the path to your folder
veh_guid = "VEHherom-otoc-orpp-ocHM-C$0000000001"                    # Replace with your vehGuid
imei = "67r8r"                           # Replace with your imei
create_zips(input_folder, veh_guid, imei)
