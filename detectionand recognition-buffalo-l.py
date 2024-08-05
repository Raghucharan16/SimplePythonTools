import os
import cv2
import numpy as np
import faiss
from insightface.app import FaceAnalysis
from scipy.spatial.distance import cosine

# Initialize the FaceAnalysis application with buffalo_l model
app = FaceAnalysis(name='buffalo_l')

# Function to get face embeddings and bounding boxes from an image
def get_face_data(image):
    faces = app.get(image)
    return [(face.embedding, face.bbox) for face in faces]

# Function to compare two face embeddings
def compare_faces(emb1, emb2, threshold=0.24):
    similarity = 1 - cosine(emb1, emb2)
    return similarity > threshold

# Process all images in a folder
def process_image_folder(input_folder, output_folder, threshold=0.24, det_thresh=0.8):
    # Prepare the model with the detection threshold
    app.prepare(ctx_id=-1, det_size=(640, 640), det_thresh=det_thresh)

    dim = 512  # Dimension of face embeddings
    index = faiss.IndexFlatL2(dim)
    
    unique_faces = []
    unique_face_images = []

    for filename in os.listdir(input_folder):
        if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            image_path = os.path.join(input_folder, filename)
            image = cv2.imread(image_path)
            if image is None:
                print(f"Failed to load image: {image_path}")
                continue

            face_data = get_face_data(image)
            
            for embedding, bbox in face_data:
                if index.ntotal == 0:
                    index.add(np.array([embedding]))
                    unique_faces.append(embedding)
                    face_image = image[int(bbox[1]):int(bbox[3]), int(bbox[0]):int(bbox[2])]
                    unique_face_images.append(face_image)
                else:
                    is_unique = True
                    for unique_emb in unique_faces:
                        if compare_faces(embedding, unique_emb, threshold):
                            is_unique = False
                            break
                    if is_unique:
                        index.add(np.array([embedding]))
                        unique_faces.append(embedding)
                        face_image = image[int(bbox[1]):int(bbox[3]), int(bbox[0]):int(bbox[2])]
                        unique_face_images.append(face_image)

    # Save unique faces
    os.makedirs(output_folder, exist_ok=True)
    for i, face_image in enumerate(unique_face_images):
        if face_image.size != 0:  # Check if the image is not empty
            output_path = os.path.join(output_folder, f"unique_face_{i}.jpg")
            cv2.imwrite(output_path, face_image)

    return len(unique_faces)

# Specify the input and output folder paths
input_folder = '/home/narravenkataraghucharan/Desktop/shuttleresults/1Images_1712293658275 (1)'
output_folder = 'output'

# Process the folder and get the number of unique faces
num_unique_faces = process_image_folder(input_folder, output_folder, threshold=0.3, det_thresh=0.8)

print(f"Number of unique faces found: {num_unique_faces}")
print(f"Unique faces saved in: {output_folder}")
