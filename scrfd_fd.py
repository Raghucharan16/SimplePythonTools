from scrfd import SCRFD, Threshold
from PIL import Image

face_detector = SCRFD.from_path("models/scrfd.onnx")
threshold = Threshold(probability=0.8)

image = Image.open("sample.jpg")
faces = face_detector.detect(image, threshold=threshold)

for face in faces:
    bbox = face.bbox
    kps = face.keypoints
    score = face.probability
    print(f"{bbox=}, {kps=}, {score=}")
