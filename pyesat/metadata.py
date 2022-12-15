from PIL import Image

def extract_metadata(image_path):
    # Open the image using the Pillow library
    img = Image.open(image_path)
    
    # Extract the metadata from the image
    metadata = {
        'filename': img.filename,
        'format': img.format,
        'size': img.size,
        'mode': img.mode,
        'date_time': img.info['date_time']
    }
    
    # Return the metadata
    return metadata