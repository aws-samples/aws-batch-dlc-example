import os, sys
from io import BytesIO
import deeplabcut
from boto3 import resource,client
import zipfile
import logging
import glob

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def import_dlc_zip_project(filekey, sourcebucketname, destination_folder):
    """ This function imports a DLC project ZIP file from an S3 bucket, and unzips the content into a local destination folder
        Usage: unzip_files(filekey, sourcebucketname, destination_folder)
             filekey: sting parameter providing the key to the ZIP object into the S3 bucket
             sourcebucketname: The name of the source bucket where the DLC project ZIP file has been uploaded.
             destination_folder: The local repository where the DLC project should be exported inside the container image
    """
    try:
        s3_resource = resource('s3')
        zipped_file = s3_resource.Object(sourcebucketname, filekey)
        
        buffer = BytesIO(zipped_file.get()["Body"].read())
        if zipfile.is_zipfile(buffer):
            zipObject = zipfile.ZipFile(buffer) 
            zipObject.extractall(destination_folder)
    except Exception as e:  
        logger.info('Error: Unable to import DLC project')
        sys.exit(1)


def export_dlc_model_s3(s3_path, destinationbucketname, local_folder="dlc-models", target_file_name="output_dlc_model.zip"):
    """ This function creates a zip file including the DLC trained model output, and exports the file to a target S3 bucket.
        Usage: export_dlc_model_s3(s3_path, destinationbucketname, local_folder)
            s3_path: The path in target s3 bucket where the trained model should be exported.
            destinationbucketname: The name of the target s3 bucket where the output model will be exported.
            (optional) local_folder: local directory where the trained DLC model is stored. Default is "dlc-models" folder in local directory.
            (optional) target_file_name: Name of target zip file to be exported to s3. Default is "output_dlc_model.zip"
    """
    try:
        s3_resource = resource('s3')
        with zipfile.ZipFile(target_file_name, 'w') as output_file:
        #    for file in glob.glob(local_folder+'/*'):
        #        output_file.write(file)
            for root, dirs, files in os.walk(local_folder):
                for file in files:
                    output_file.write(os.path.join(root, file))
        s3_resource.meta.client.upload_file(target_file_name,destinationbucketname,s3_path+target_file_name)
    except Exception as e:
        logger.info('Error: Unable to export the trained DLC model into target s3 bucket')
        sys.exit(1)

def train_dlc_model(config_file):
    """ This function invokes the native deeplabcut training modules. This includes the functional part to be defined by the researcher.
        We illustrate here a simple example code, invoking the function deeplabcut.train_network using default arguments.
        Usage: train_dlc_model(config_file)
            config_file: The path to the deeplabcut YAML config file.
    """
    try:
        # Training the model
        deeplabcut.train_network(config, shuffle=shuffle, max_snapshots_to_keep=5, maxiters=Maxiter)
        # Evaluating the model
        deeplabcut.evaluate_network(config, Shuffles=[shuffle],plotting=True)
    except Exception as e:
        logger.info('Error: Failed executing the deeplabcut train_network function')
        sys.exit(1)

def main():
    dlc_project_path = ""
    dlc_model_output_path = ""
    tgt_s3_bucket = ""
    local_project_path = ""
    config = ""
    # Initializing environment variables
    try:
        dlc_project_path = os.environ['DLC_PROJECT_PATH'] # Defined as environment variable at task creation in AWS Console
        tgt_s3_bucket = os.environ['TGT_S3_BUCKET'] # Defined as environment variable at task creation in AWS Console
        local_project_path = os.environ['LOCAL_PROJECT_PATH'] # Created inside Dockerfile
        dlc_model_output_path = os.environ['OUTPUT_PATH'] # Defined as environment variable at task creation in AWS Console
        config = os.environ['config_path'] # Created inside Dockerfile
    except Exception as e:
        logger.info('KeyError: environment variables are not properly created')
        sys.exit(1)

    # Importing deeplabcut project into local directory
    import_dlc_zip_project(dlc_project_path, tgt_s3_bucket, local_project_path)
    
    # Executing the training function
    train_dlc_model(config)

    # Exporting deeplabcut trained models into s3 bucket
    outout_dlc_model_path = local_project_path+"/dlc-models" # dlc-models is the output directory where deeplabcut exports dlc models
    export_dlc_model_s3(dlc_model_output_path, tgt_s3_bucket, outout_dlc_model_path)

if __name__ == "__main__":
    main()

