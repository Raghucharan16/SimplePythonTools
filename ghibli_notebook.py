# Ghibli Style Image Generator
# For Google Colab with Gradio interface

!pip install -q diffusers transformers accelerate gradio

import torch
import gradio as gr
from PIL import Image
from diffusers import StableDiffusionImg2ImgPipeline
from io import BytesIO

# Check if GPU is available
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {device}")

# Function to load model (only when needed to save memory)
@torch.no_grad()
def load_model():
    # Using a model fine-tuned for anime/Ghibli style
    model_id = "nitrosocke/Ghibli-Diffusion"
    
    pipe = StableDiffusionImg2ImgPipeline.from_pretrained(
        model_id,
        torch_dtype=torch.float16 if device == "cuda" else torch.float32,
        safety_checker=None
    )
    
    # Move to GPU if available
    pipe = pipe.to(device)
    
    # Enable memory optimizations
    if device == "cuda":
        pipe.enable_attention_slicing()
        
    return pipe

# Function to generate Ghibli-style image
def generate_ghibli_image(input_image, prompt_strength, guidance_scale, steps):
    # Load the model for this specific run
    pipe = load_model()
    
    # Resize image if too large (to save memory)
    width, height = input_image.size
    max_size = 768
    if width > max_size or height > max_size:
        if width > height:
            new_width = max_size
            new_height = int(max_size * height / width)
        else:
            new_height = max_size
            new_width = int(max_size * width / height)
        input_image = input_image.resize((new_width, new_height))
    
    # Prepare prompt
    prompt = "studio ghibli style, ghibli anime style, hayao miyazaki style, anime style"
    
    # Generate image
    try:
        with torch.no_grad():
            result = pipe(
                prompt=prompt,
                image=input_image,
                strength=prompt_strength,
                guidance_scale=guidance_scale,
                num_inference_steps=steps
            ).images[0]
        
        # Clean up GPU memory
        del pipe
        if device == "cuda":
            torch.cuda.empty_cache()
            
        return result
    except Exception as e:
        print(f"Error generating image: {e}")
        # Clean up GPU memory
        del pipe
        if device == "cuda":
            torch.cuda.empty_cache()
        return None

# Create Gradio interface
def main():
    with gr.Blocks() as demo:
        gr.Markdown("# Studio Ghibli Style Image Generator")
        gr.Markdown("Upload an image and convert it to Studio Ghibli animation style")
        
        with gr.Row():
            with gr.Column():
                input_image = gr.Image(type="pil", label="Upload Image")
                prompt_strength = gr.Slider(0.1, 0.9, 0.6, step=0.05, label="Transformation Strength")
                guidance_scale = gr.Slider(2, 12, 7.5, step=0.5, label="Guidance Scale")
                steps = gr.Slider(20, 50, 30, step=5, label="Steps")
                submit_btn = gr.Button("Generate Ghibli Style Image")
            
            with gr.Column():
                output_image = gr.Image(label="Ghibli Style Result")
        
        # Connect the function
        submit_btn.click(
            fn=generate_ghibli_image,
            inputs=[input_image, prompt_strength, guidance_scale, steps],
            outputs=output_image
        )
        
        gr.Markdown("""
        ## How to use:
        1. Upload an image
        2. Adjust parameters:
           - **Transformation Strength**: How much to transform the image (higher = more Ghibli-like but less like original)
           - **Guidance Scale**: How closely to follow the prompt (higher = more stylistic)
           - **Steps**: More steps = better quality but slower
        3. Click "Generate Ghibli Style Image"
        
        ## Note:
        - First generation may take a while as the model loads
        - If you encounter memory issues, try a smaller image or reduce steps
        """)
    
    # Launch the interface
    demo.launch(debug=True)

# Run the app
if __name__ == "__main__":
    main()
