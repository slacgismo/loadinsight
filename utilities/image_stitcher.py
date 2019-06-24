import os
from PIL import Image

def merge_images(file1, file2, direction):
    """Merge two images into one, displayed side by side
    :param file1: path to first image file
    :param file2: path to second image file
    :return: the merged Image object
    """
    try:
        image1 = Image.open(file1)
    except:
        image1 = file1
    image2 = Image.open(file2)

    (width1, height1) = image1.size
    (width2, height2) = image2.size

    if direction == 'horizontal':
        result_width = width1 + width2
        result_height = max(height1, height2)
    else:
        result_width = max(width1, width2)
        result_height = height1 + height2

    result = Image.new('RGB', (result_width, result_height))
    result.paste(im=image1, box=(0, 0))

    if direction == 'horizontal':
        result.paste(im=image2, box=(width1, 0))
    else:
        result.paste(im=image2, box=(0, height1))

    return result

def stitch(directory_path, output_name):
    # FIXME: remove this while loop and leverage the directory listing appropriately
    plots = os.listdir(directory_path)

    x = 0
    image = None
    new_row = True

    while new_row:
        y = 0
        image_h = None
        new_column = True
        
        while new_column:
            file = f'{str(x)}_{str(y)}.png'
            filepath = f'{directory_path}{file}'

            if image_h:
                if file in plots:
                    image_h = merge_images(image_h, filepath, 'horizontal')
                else:
                    new_column = False
            elif file in plots:
                image_h = Image.open(filepath)
            else:
                new_row = False
                break

            mid_path = f'{directory_path}{str(x)}.png'
            image_h.save(mid_path) 

            y += 1

        if image:
            image = merge_images(image, mid_path, 'vertical')
        else:
            image = image_h

        x += 1

    image.save(output_name)