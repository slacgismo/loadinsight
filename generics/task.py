import logging
from time import time
from generics import artifact


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Task(artifact.ArtifactDataManager):
    """
    Generic task manager that implements a function and executes it on
    a set of given inputs and generates a set of outputs.

    Inputs may be data files
    Outputs may also be data files
    """
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.run_result = None
        self.task_function = None
        self.task_end_time = None
        self.task_start_time = None
        self.did_task_pass_validation = True
        self.task_results = None

    def _get_time(self):
        return time()

    def get_task_run_time(self):
        try:
            return round((self.task_end_time - self.task_start_time) / 60, 2)
        except TypeError as e:
            logger.warning('Could not deduce task run time, returning zero')
            return 0

    def run(self):
        # set the start time for this run
        self.task_start_time = self._get_time()

        logger.info(f'running task {self.name}')
        if self.task_function:
            self.run_result = self.task_function()
        else:
            raise TypeError(f'{self.name} does not implement a function that this <Task> can execute')

        # set the end time for this run
        self.task_end_time = self._get_time()

    def on_complete(self, data_map):
        """
        Given a dictionary of filenames and dataframes, we'll generate hashes for these entities
        Perform a conflict resolution and save the output when necessary
        input:
            { filename: df, filename_2: df_2, ... }
        output:
            [{
                'output_artifact': filename,
                'output_data_hash': df,
            }]
        """
        results = []

        for output_filename, data_frame in data_map.items():
            new_filename = None
            new_file_contents_hex_digest = None
            existing_file_contents_hex_digest = None
            
            # get the hash of the pandas data frame 
            # this will be used as the name for the temp file if needed
            df_hex_digest = self.get_data_frame_hash(data_frame)
            
            logger.info(f'Existing df hash {df_hex_digest}')
            
            # determine if the file we want to write already exists and if so, compare hashes with the
            # file we're about to write. We can't assume data hashes will match for the same data frame
            # when it gets loaded from a file...
            if self.does_file_exist(output_filename):
                logger.info(f'Checking hash of file that already exists {output_filename}')
                existing_file_contents_hex_digest = self.check_file_contents_hash(output_filename)
                
                # ...therefore we'll temporarily write a file based on the data hash
                # and determine its file content's hash
                new_filename = f'{output_filename}__{df_hex_digest}__.csv'
                self.save_data(new_filename, data_frame)
                new_file_contents_hex_digest = self.check_file_contents_hash(new_filename)

                if new_file_contents_hex_digest == existing_file_contents_hex_digest:
                    # since they are the same, we don't do anything, just cleanup
                    logger.info('The hashes matched, deleting file...')
                    self.delete_file(new_filename)
                    new_filename = None
                    new_file_contents_hex_digest = None
            else:
                self.save_data(output_filename, data_frame)

            results.append({
                'output_filename': output_filename,
                'data_frame_hash': df_hex_digest,
                'existing_file_hash': existing_file_contents_hex_digest,
                'new_filename': new_filename,
                'new_file_hash': new_file_contents_hex_digest
            })

        self.task_results = results

    def on_failure(self):
        logger.info('Cleanup at the task level')
