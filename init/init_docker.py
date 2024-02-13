"""This module is used to initialize docker servers."""

from loguru import logger
from subprocess import call

class init_docker(object):
    """ Class init_docker used to initialize the all docker servers.
    
    Methods
    ----------
    start() -> None:
        Starts the process of initialize the all docker servers.
    """
    def __create_docker_server(self, file, slash:str):
        call(['docker-compose', '-f', 
              f'docker{slash}yml{slash}docker-compose.yml', 'up', '-d', '--build'], 
             stdout=file, stderr=file, stdin=file)
        
    def start(self, path_to_log, slash):
        """The method starts initialization of the all docker servers.
        
        Arguments
        --------------
            path_to_log: str - Path to the log file.
            slash: str - Type of slash symbol used in paths.
        
        Functionality
        --------------
        Expected result:
            Created and launched redis and kafka servers.
            
        Exception:
            If run on any operating system other than win and linux, errors may occur.
            If changes are made to the requirements file, errors may occur.
            If you delete or move the logs directory, an error will occur.
        """
        logger.info("Start work init_docker function.\n")
        try:
            with open(path_to_log, 'a') as file:
                self.__create_docker_server(file, slash)
                logger.info(f"Creation and launching of docker servers is completed.\n")
        except Exception as e:
            logger.error(e)
        logger.info("End work init_docker function.\n")
    
if __name__ == "__main__":
    logger.error("The init_docker.py module cannot be run by itself, use the bild.py module to build the project")