import os
from retinaface import RetinaFace
import cv2

def generate_faces(input_folder, output_folder):
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Iterate through each image in the input folder
    for filename in os.listdir(input_folder):
        # Construct the path to the input image
        input_image_path = os.path.join(input_folder, filename)

        # Load the input image
        image = cv2.imread(input_image_path)

        # Detect faces in the image using RetinaFace
        faces = RetinaFace.detect_faces(input_image_path)

        # Extract and save each detected face
        for i, face in faces.items():
            # Extract the coordinates of the face bounding box
            x1, y1, x2, y2 = face['facial_area']

            # Crop the face region from the image
            face_image = image[y1:y2, x1:x2]

            # Construct the path to save the face image
            output_face_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}_face_{i}.jpg")

            # Save the face image
            cv2.imwrite(output_face_path, face_image)

            print(f"Face {i+1} from {filename} saved as {output_face_path}")

# Example usage
input_folder = "/path/to/input/folder"
output_folder = "/path/to/output/folder"
generate_faces(input_folder, output_folder)
