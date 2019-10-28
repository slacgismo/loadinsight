import os
import logging
from PIL import Image

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')

def stitch(directory_path, output_name, cols=3):
    """
    Given a directory, get all available png's and collate them in a row x column setup 
    with the supplied number of columns. The rows are deduced as a fn of the number of 
    pngs in the directory, divided by the given number of columns.
    Assumptions:
    - All images are the same size - we are simply getting the first image in the dir
      and using its width and height for all subsequent calculations
    """
    img_width = 0
    img_height = 0
    final_image = None
    plots = sorted(os.listdir(directory_path))
    number_of_images = len(plots)

    logger.info(f'Stitching {str(number_of_images)} images in a {str(cols)}x{str(int(number_of_images / cols))} collage')

    for i in range(number_of_images):
        plot_image_filename = os.fsdecode(plots[i])
        
        if not plot_image_filename.endswith('.png'):
            # FIXME: this should raise an exception otherwise it'll screw up the image collage
            # since it assumes height based on total number of files in dir, vs total number of
            # png's in dir. We can account for this later.
            logger.info('Encountered an image other than .png, skipping processing...')
            continue
            
        img = Image.open(f'{directory_path}{plot_image_filename}')

        if i == 0:
            # this is the first iteration. We need to create the new image where all
            # the others will be collated. This is determined by the relative dimensions
            # of the first available image in the directory
            (img_width, img_height) = img.size
            # determine the final collage's dimensions
            final_image_width = img_width * cols
            final_image_height = img_height * (int(number_of_images / cols))
            final_image = Image.new('RGB', (final_image_width, final_image_height))
        
        x_position = int(i / cols)
        y_position = i % cols
        final_image.paste(img, (img_width * y_position, img_height * x_position))

    if final_image:
        final_image.save(f'{directory_path}{output_name}')
